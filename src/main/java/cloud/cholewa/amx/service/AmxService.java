package cloud.cholewa.amx.service;

import cloud.cholewa.amx.device.client.DeviceDatabaseClient;
import cloud.cholewa.amx.eaton.model.PayloadType;
import cloud.cholewa.amx.frameserver.ReceivedFrame;
import cloud.cholewa.amx.rabbit.DeviceEventPublisher;
import cloud.cholewa.amx.rabbit.TemperaturePublisher;
import cloud.cholewa.home.model.EatonConfigurationResponse;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

import static cloud.cholewa.amx.eaton.utils.MessageUtilities.extractDataPoint;
import static cloud.cholewa.amx.eaton.utils.MessageUtilities.extractMessage;
import static cloud.cholewa.amx.eaton.utils.MessageValidator.isValidEatonMessage;
import static cloud.cholewa.amx.eaton.utils.device.DeviceEventParser.parse;
import static cloud.cholewa.amx.eaton.utils.device.TemperatureParser.calculateRoomTemperature;

/**
 * What is done with a frame a gateway gave the master. The frame server forwards every frame, so
 * the type byte decides first: an RX frame ({@code C1}, a device reporting) is looked up in
 * {@code database-service} by its data point and published for the service that acts on it; a
 * STATUS frame ({@code C3}) is the gateway's answer to a command - this service's or the
 * Bedroom master's - and carries no data point: the telegram was sent, or it was not and the
 * gateway says why; an ERROR frame ({@code C9}) is a text of the gateway. A frame that is none
 * of these, or one the parser refuses, ends in an
 * {@link cloud.cholewa.amx.infrastructure.error.EatonParsingException}.
 */
@Service
@Slf4j
@RequiredArgsConstructor
public class AmxService {

    //the type byte follows the start byte and the length; a status frame then carries the status
    //type and the status
    private static final int TYPE_INDEX = 2;
    private static final int STATUS_TYPE_INDEX = 3;
    private static final int STATUS_INDEX = 4;

    //STT_OK with STS_OK_MRF: the gateway took the telegram. The data bytes after them vary
    private static final String STATUS_OK = "1C";
    private static final String STATUS_OK_MRF = "04";
    //STT_ERROR: the telegram was not sent, the status says why
    private static final String STATUS_ERROR = "09";
    private static final Map<String, String> ERROR_REASONS = Map.of(
        "01", "unknown command",
        "02", "data point out of range",
        "03", "RF busy, the telegram was lost",
        "04", "RF busy receiving"
    );

    private final DeviceDatabaseClient deviceDatabaseClient;
    private final TemperaturePublisher temperaturePublisher;
    private final DeviceEventPublisher deviceEventPublisher;

    public Mono<Void> consume(final ReceivedFrame received) {
        return Mono.fromSupplier(() -> received.frame().strip().toUpperCase(Locale.ROOT))
            .flatMap(frame -> {
                final String[] parts = frame.split(",");
                //32 bytes of text, which the validator does not know: told apart before it
                if (is(PayloadType.ERROR, parts)) {
                    log.warn("Gateway {} reported an error: [{}]", received.gateway(), frame);
                    return Mono.empty();
                }
                return Mono.fromCallable(() -> isValidEatonMessage(frame))
                    .filter(Boolean::booleanValue)
                    .flatMap(isValid -> is(PayloadType.STATUS, parts)
                        ? status(received, frame, parts)
                        : lookUpAndPublish(received, frame));
            });
    }

    private static boolean is(final PayloadType type, final String[] parts) {
        return parts.length > TYPE_INDEX && type.getValue().equals(parts[TYPE_INDEX]);
    }

    //the gateway's answer to a command: this service's, or the Bedroom master's on the same port
    private static Mono<Void> status(final ReceivedFrame received, final String frame, final String[] parts) {
        return Mono.fromRunnable(() -> {
            final String statusType = parts[STATUS_TYPE_INDEX];
            final String status = parts[STATUS_INDEX];
            if (STATUS_OK.equals(statusType) && STATUS_OK_MRF.equals(status)) {
                log.info("Gateway {} took a telegram: [{}]", received.gateway(), frame);
            } else if (STATUS_ERROR.equals(statusType)) {
                log.warn(
                    "Gateway {} did not send a telegram: {} [{}]",
                    received.gateway(), ERROR_REASONS.getOrDefault(status, "error " + status), frame
                );
            } else {
                log.info("Gateway {} sent a status of type {}: [{}]", received.gateway(), statusType, frame);
            }
        });
    }

    private Mono<Void> lookUpAndPublish(final ReceivedFrame received, final String frame) {
        return Mono.fromSupplier(() -> extractMessage(frame))
            .zipWhen(message ->
                deviceDatabaseClient.getEatonConfiguration(extractDataPoint(message), received.gateway()))
            .flatMap(t -> publish(received, t.getT1(), t.getT2()));
    }

    //what the registry says the point is decides where the reading goes: a room temperature to
    //heating-service, what an actuator reports - its status after a command (70), a button
    //pressed - to the services of the devices, as the datagram names it. OTHER is no device this
    //service can read: a line in the log, not a failure, the frames keep coming
    private Mono<Void> publish(
        final ReceivedFrame received,
        final List<String> message,
        final EatonConfigurationResponse configuration
    ) {
        return switch (Objects.requireNonNull(configuration.getType())) {
            case TEMPERATURE_SENSOR -> Mono.fromRunnable(() ->
                    log.info("Publishing message type: {} for room: {}", configuration.getType(), configuration.getRoom()))
                .then(temperaturePublisher.publish(
                    calculateRoomTemperature(message), configuration.getRoom(), received.receivedAt()));
            case BLINDS, LIGHT, DIMMER -> Mono.fromSupplier(() -> parse(message, received.gateway(), received.receivedAt()))
                .doOnNext(event -> log.info(
                    "Publishing {} of a {} in room {} on point {}",
                    event.getEvent(), configuration.getType(), configuration.getRoom(), event.getPoint()
                ))
                .flatMap(deviceEventPublisher::publish);
            case OTHER -> Mono.fromRunnable(() -> log.warn(
                "Frame of a device of the type {} in room {} not published: no event is defined for it",
                configuration.getType(), configuration.getRoom()
            ));
        };
    }
}

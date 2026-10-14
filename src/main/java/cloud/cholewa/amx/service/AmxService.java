package cloud.cholewa.amx.service;

import cloud.cholewa.amx.device.client.DeviceDatabaseClient;
import cloud.cholewa.amx.eaton.model.PayloadType;
import cloud.cholewa.amx.frameserver.ReceivedFrame;
import cloud.cholewa.amx.rabbit.TemperaturePublisher;
import cloud.cholewa.home.model.EatonConfigurationResponse;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.Locale;
import java.util.Objects;

import static cloud.cholewa.amx.eaton.utils.MessageUtilities.extractDataPoint;
import static cloud.cholewa.amx.eaton.utils.MessageUtilities.extractMessage;
import static cloud.cholewa.amx.eaton.utils.MessageValidator.isValidEatonMessage;
import static cloud.cholewa.amx.eaton.utils.device.TemperatureParser.calculateRoomTemperature;

/**
 * What is done with a frame a gateway gave the master. The frame server forwards every frame, so
 * the type byte decides first: an RX frame ({@code C1}, a device reporting) is looked up in
 * {@code database-service} by its data point and published for the service that acts on it; a
 * STATUS frame ({@code C3}) is the gateway's acknowledgement of a command - this service's or the
 * Bedroom master's - and carries no data point; an ERROR frame ({@code C9}) is a text of the
 * gateway. A frame that is none of these, or one the parser refuses, ends in an
 * {@link cloud.cholewa.amx.infrastructure.error.EatonParsingException}.
 */
@Service
@Slf4j
@RequiredArgsConstructor
public class AmxService {

    //the type byte follows the start byte and the length
    private static final int TYPE_INDEX = 2;

    private final DeviceDatabaseClient deviceDatabaseClient;
    private final TemperaturePublisher temperaturePublisher;

    public Mono<Void> consume(final ReceivedFrame received) {
        return Mono.fromSupplier(() -> received.frame().strip().toUpperCase(Locale.ROOT))
            .flatMap(frame -> {
                final String type = typeOf(frame);
                if (PayloadType.STATUS.getValue().equals(type)) {
                    log.info("Gateway {} acknowledged a command: [{}]", received.gateway(), frame);
                    return Mono.empty();
                }
                if (PayloadType.ERROR.getValue().equals(type)) {
                    log.warn("Gateway {} reported an error: [{}]", received.gateway(), frame);
                    return Mono.empty();
                }
                return Mono.fromCallable(() -> isValidEatonMessage(frame))
                    .filter(Boolean::booleanValue)
                    .map(isValid -> extractMessage(frame))
                    .zipWhen(message ->
                        deviceDatabaseClient.getEatonConfiguration(extractDataPoint(message), received.gateway()))
                    .flatMap(t -> publish(t.getT1(), t.getT2()));
            });
    }

    private static String typeOf(final String frame) {
        final String[] parts = frame.split(",");
        return parts.length > TYPE_INDEX ? parts[TYPE_INDEX] : "";
    }

    //what the registry says the point is decides where the reading goes. The actuators report
    //too - their status after a command (70), a button pressed - and until HAS-229 nobody takes
    //those: a line in the log, not a failure, the frames keep coming
    private Mono<Void> publish(final List<String> message, final EatonConfigurationResponse configuration) {
        return switch (Objects.requireNonNull(configuration.getType())) {
            case TEMPERATURE_SENSOR -> Mono.fromRunnable(() ->
                    log.info("Publishing message type: {} for room: {}", configuration.getType(), configuration.getRoom()))
                .then(temperaturePublisher.publish(calculateRoomTemperature(message), configuration.getRoom()));
            case BLINDS, LIGHT, DIMMER, OTHER -> Mono.fromRunnable(() -> log.warn(
                "Frame of a {} in room {} not published: device events are not supported until HAS-229",
                configuration.getType(), configuration.getRoom()
            ));
        };
    }
}

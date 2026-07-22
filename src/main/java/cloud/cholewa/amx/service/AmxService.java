package cloud.cholewa.amx.service;

import cloud.cholewa.amx.device.client.DeviceDatabaseClient;
import cloud.cholewa.amx.rabbit.TemperaturePublisher;
import cloud.cholewa.home.model.EatonConfigurationResponse;
import cloud.cholewa.home.model.EatonDatagramReply;
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

@Service
@Slf4j
@RequiredArgsConstructor
public class AmxService {

    private final DeviceDatabaseClient deviceDatabaseClient;
    private final TemperaturePublisher temperaturePublisher;

    public Mono<Void> consumeAmxMessage(final EatonDatagramReply reply) {
        return Mono.justOrEmpty(reply.getMessage().trim().toUpperCase(Locale.ROOT))
            .flatMap(frame ->
                Mono.fromCallable(() -> isValidEatonMessage(frame))
                    .filter(Boolean::booleanValue)
                    .map(isValid -> extractMessage(frame))
                    .zipWhen(message ->
                        deviceDatabaseClient.getEatonConfiguration(extractDataPoint(message), reply.getGateway()))
                    .doOnNext(t ->
                        log.info("Publishing message type: {} for room: {}", t.getT2().getType(), t.getT2().getRoom()))
                    .flatMap(t -> publishOnRabbit(t.getT1(), t.getT2()))
            );
    }

    private Mono<Void> publishOnRabbit(final List<String> message, final EatonConfigurationResponse configuration) {
        return switch (Objects.requireNonNull(configuration.getType())) {
            case TEMPERATURE_SENSOR ->
                temperaturePublisher.publish(calculateRoomTemperature(message), configuration.getRoom());
            case BLINDS -> Mono.error(new RuntimeException("Blinds are not supported yet"));
            case LIGHT -> Mono.error(new RuntimeException("Lights are not supported yet"));
            case DIMMER -> Mono.error(new RuntimeException("Dimmers are not supported yet"));
            case OTHER -> Mono.error(new RuntimeException("Other devices are not supported yet"));
        };
    }
}

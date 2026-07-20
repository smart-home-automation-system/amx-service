package cloud.cholewa.gateway.service;

import cloud.cholewa.gateway.device.client.DeviceDatabaseClient;
import cloud.cholewa.gateway.rabbit.TemperaturePublisher;
import cloud.cholewa.home.model.EatonConfigurationResponse;
import cloud.cholewa.home.model.EatonDatagramReply;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.Objects;

import static cloud.cholewa.gateway.eaton.utils.MessageUtilities.extractDataPoint;
import static cloud.cholewa.gateway.eaton.utils.MessageUtilities.extractMessage;
import static cloud.cholewa.gateway.eaton.utils.MessageValidator.isValidEatonMessage;
import static cloud.cholewa.gateway.eaton.utils.device.TemperatureParser.calculateRoomTemperature;

@Service
@Slf4j
@RequiredArgsConstructor
public class GatewayService {

    private final DeviceDatabaseClient deviceDatabaseClient;
    private final TemperaturePublisher temperaturePublisher;

    public Mono<Void> consumeAmxMessage(final EatonDatagramReply reply) {
        return Mono.fromCallable(() -> isValidEatonMessage(reply.getMessage()))
            .filter(valid -> valid)
            .map(valid -> extractMessage(reply.getMessage()))
            .zipWhen(message ->
                deviceDatabaseClient.getEatonConfiguration(extractDataPoint(message), reply.getGateway()))
            .doOnNext(tuple ->
                log.info("Publishing message type: {} for room: {}", tuple.getT2().getType(), tuple.getT2().getRoom()))
            .flatMap(tuple -> publishOnRabbit(tuple.getT1(), tuple.getT2()));
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

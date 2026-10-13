package cloud.cholewa.amx.service;

import cloud.cholewa.amx.device.registry.EatonDeviceRegistry;
import cloud.cholewa.amx.eaton.frame.BlindsCommand;
import cloud.cholewa.amx.eaton.frame.EatonFrameBuilder;
import cloud.cholewa.amx.frameserver.FrameServerClient;
import cloud.cholewa.amx.infrastructure.error.CustomError;
import cloud.cholewa.amx.infrastructure.error.EatonCommandException;
import cloud.cholewa.home.model.DeviceKind;
import cloud.cholewa.home.model.LightKind;
import cloud.cholewa.home.model.SmartDevice;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

import java.time.Clock;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;

//The way from the backend to an Eaton actuator: the registry says on which gateway the point is,
//the frame is built here and handed to the frame server of the AMX master. What comes back is the
//time the server took the frame - whether the blind moved is told by the datagram of the actuator,
//not by this call
@Service
@Slf4j
@RequiredArgsConstructor
public class EatonCommandService {

    private final EatonDeviceRegistry eatonDeviceRegistry;
    private final FrameServerClient frameServerClient;
    private final Clock clock;

    public Mono<LocalDateTime> moveBlinds(final int point, final BlindsCommand command) {
        return eatonDeviceRegistry.find(DeviceKind.BLINDS, point)
            .flatMap(device -> send(device, EatonFrameBuilder.blinds(point, command)));
    }

    public Mono<LocalDateTime> switchLight(final int point, final boolean on) {
        return eatonDeviceRegistry.find(DeviceKind.LIGHT, point)
            .flatMap(device -> send(device, EatonFrameBuilder.lightSwitch(point, on)));
    }

    //a level is for a light the registry calls a dimmer; a light without a light kind is not one
    public Mono<LocalDateTime> setLightLevel(final int point, final int level) {
        return eatonDeviceRegistry.find(DeviceKind.LIGHT, point)
            .flatMap(device -> device.getLightKind() == LightKind.DIMMER
                ? send(device, EatonFrameBuilder.lightLevel(point, level))
                : Mono.error(new EatonCommandException(
                    HttpStatus.CONFLICT,
                    CustomError.NOT_A_DIMMER,
                    "The light on point: " + point + " is not registered as a dimmer"
                )));
    }

    private Mono<LocalDateTime> send(final SmartDevice device, final String frame) {
        return frameServerClient.send(device.getEaton().getGateway(), frame)
            .doOnSubscribe(subscription -> log.info(
                "Sending frame [{}] to gateway: {} for device: {} in room: {}",
                frame, device.getEaton().getGateway(), device.getName(), device.getRoom()
            ))
            .then(Mono.fromSupplier(() -> LocalDateTime.now(clock).truncatedTo(ChronoUnit.SECONDS)));
    }
}

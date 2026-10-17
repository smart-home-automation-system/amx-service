package cloud.cholewa.amx.api;

import cloud.cholewa.amx.api.model.FrameSentResponse;
import cloud.cholewa.amx.eaton.frame.BlindsCommand;
import cloud.cholewa.amx.eaton.frame.EatonFrameBuilder;
import cloud.cholewa.amx.service.EatonCommandService;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ServerWebInputException;
import reactor.core.publisher.Mono;

import java.time.LocalDateTime;

//The commands for the Eaton actuators. 202, not 200: the answer says the frame was handed to the
//gateway, never that the device did what it was told.
//No @Validated on the class - WebFlux validates the constrained parameters itself, and the
//annotation would switch that off for an AOP proxy. The messages are written on the constraints,
//so the answer reads the same on every machine
@RestController
@RequestMapping("/amx/eaton")
@Slf4j
@RequiredArgsConstructor
public class EatonCommandController {

    private static final String POINT_RANGE = "point must be between 1 and 99";
    private static final String TURN_ON = "on";
    private static final String TURN_OFF = "off";

    private final EatonCommandService eatonCommandService;

    @PostMapping("/blinds/{point}")
    Mono<ResponseEntity<FrameSentResponse>> moveBlinds(
        @PathVariable
        @Min(value = EatonFrameBuilder.MIN_POINT, message = POINT_RANGE)
        @Max(value = EatonFrameBuilder.MAX_POINT, message = POINT_RANGE) final int point,
        @RequestParam final String command
    ) {
        final BlindsCommand blindsCommand = BlindsCommand.ofParameter(command)
            .orElseThrow(() -> new ServerWebInputException("command must be open, close or stop"));

        return eatonCommandService.moveBlinds(point, blindsCommand)
            .doOnSubscribe(subscription -> log.info("Blinds on point: {} asked to: {}", point, command))
            .map(EatonCommandController::accepted);
    }

    //a status request is not a move, so it is a path of its own and not a fourth command: it
    //sends 0B, never 0E. The 202 says the request went out; the state arrives as a status event
    @PostMapping("/blinds/{point}/status")
    Mono<ResponseEntity<FrameSentResponse>> requestBlindsStatus(
        @PathVariable
        @Min(value = EatonFrameBuilder.MIN_POINT, message = POINT_RANGE)
        @Max(value = EatonFrameBuilder.MAX_POINT, message = POINT_RANGE) final int point
    ) {
        return eatonCommandService.requestBlindsStatus(point)
            .doOnSubscribe(subscription -> log.info("Blinds on point: {} asked for its status", point))
            .map(EatonCommandController::accepted);
    }

    //on or off and nothing else: a typo must not switch a light off
    @PostMapping("/lights/{point}")
    Mono<ResponseEntity<FrameSentResponse>> switchLight(
        @PathVariable
        @Min(value = EatonFrameBuilder.MIN_POINT, message = POINT_RANGE)
        @Max(value = EatonFrameBuilder.MAX_POINT, message = POINT_RANGE) final int point,
        @RequestParam final String turn
    ) {
        if (!TURN_ON.equals(turn) && !TURN_OFF.equals(turn)) {
            throw new ServerWebInputException("turn must be on or off");
        }

        return eatonCommandService.switchLight(point, TURN_ON.equals(turn))
            .doOnSubscribe(subscription -> log.info("Light on point: {} asked to turn: {}", point, turn))
            .map(EatonCommandController::accepted);
    }

    @PostMapping("/lights/{point}/level")
    Mono<ResponseEntity<FrameSentResponse>> setLightLevel(
        @PathVariable
        @Min(value = EatonFrameBuilder.MIN_POINT, message = POINT_RANGE)
        @Max(value = EatonFrameBuilder.MAX_POINT, message = POINT_RANGE) final int point,
        @RequestParam
        @Min(value = 0, message = "value must be between 0 and 100")
        @Max(value = EatonFrameBuilder.MAX_LEVEL, message = "value must be between 0 and 100") final int value
    ) {
        return eatonCommandService.setLightLevel(point, value)
            .doOnSubscribe(subscription -> log.info("Light on point: {} asked for level: {}", point, value))
            .map(EatonCommandController::accepted);
    }

    private static ResponseEntity<FrameSentResponse> accepted(final LocalDateTime sentAt) {
        return ResponseEntity.accepted().body(new FrameSentResponse(sentAt));
    }
}

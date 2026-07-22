package cloud.cholewa.amx.api;

import cloud.cholewa.amx.service.AmxService;
import cloud.cholewa.home.model.EatonDatagramReply;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Mono;

@RestController
@RequestMapping("/amx")
@Slf4j
@RequiredArgsConstructor
public class AmxController {

    private final AmxService amxService;

    @PostMapping
    Mono<ResponseEntity<Void>> consumeAmxMessage(@RequestBody @Valid EatonDatagramReply reply) {
        return amxService.consumeAmxMessage(reply)
            .doOnSubscribe(subscription ->
                log.info(
                    "Consuming AMX message from module: {} and content: [{}]",
                    reply.getGateway(),
                    reply.getMessage()
                ))
            .then(Mono.fromCallable(() -> ResponseEntity.ok().build()));
    }
}

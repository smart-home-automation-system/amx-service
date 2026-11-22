package cloud.cholewa.amx.service;

import cloud.cholewa.amx.panelserver.PanelServerClient;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

import java.time.Clock;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;

//the commands of the entrance panel on the Office master (HAS-484)
@Slf4j
@Service
@RequiredArgsConstructor
public class PanelCommandService {

    private final PanelServerClient panelServerClient;
    private final Clock clock;

    //wakes the panel and shows the doorbell page - no sound, ever (owner, 2026-11-22). Answers when
    //the master took it, to the second
    public Mono<LocalDateTime> ring() {
        return panelServerClient.ring()
            .doOnSubscribe(subscription -> log.info("Ringing the entrance panel"))
            .then(Mono.fromSupplier(() -> LocalDateTime.now(clock).truncatedTo(ChronoUnit.SECONDS)));
    }
}

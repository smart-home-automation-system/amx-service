package cloud.cholewa.amx.panelserver;

import cloud.cholewa.amx.gate.GateClient;
import io.micrometer.observation.Observation;
import io.micrometer.observation.ObservationRegistry;
import io.micrometer.observation.contextpropagation.ObservationThreadLocalAccessor;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import reactor.core.Disposable;
import reactor.core.publisher.Mono;

/**
 * The gate button of the entrance panel (HAS-487, epic HAS-479): a press arrives from the panel
 * server as {@code gate-open}, the driveway gate is opened through gate-service ({@link GateClient},
 * {@code source=panel}) and the panel is told the result - {@code gate;ok} while the gate is opening
 * (an opening of the last seconds included), {@code gate;failed} otherwise.
 * <p>
 * One press at a time, each under an observation of its own (a press has no request to take a trace
 * from), so its lines and the call of gate-service share one {@code traceId}; a press while one is
 * handled is dropped, never queued - a queued press could open the gate long after it was made.
 * A press while the panel socket is down never arrives - lost by design; a result the panel cannot be
 * told any more is a WARN. Nothing is ever repeated.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class PanelGateButton {

    static final String OBSERVATION = "panel.gate";

    private final PanelServerClient panel;
    private final GateClient gateClient;
    private final ObservationRegistry observationRegistry;

    private Disposable subscription;

    //before the lifecycle connects the panel socket: no press is missed for want of a subscriber
    @PostConstruct
    void subscribe() {
        subscription = panel.panelLines()
            .filter(PanelLine.GATE_OPEN::equals)
            //one at a time, and never a queue: a press kept waiting behind a gate-service that hangs
            //would open the gate long after the person left (review of HAS-487). A press while one is
            //handled is dropped - the panel gets the result of the first
            .onBackpressureDrop(press -> log.info("A press of the gate button of the entrance panel ignored: the one before it is still handled"))
            .concatMap(press -> press())
            .subscribe(
                nothing -> { },
                failure -> log.error("The gate button of the entrance panel ended: no press is handled any more", failure),
                () -> log.info("The gate button of the entrance panel ended with the client")
            );
    }

    @PreDestroy
    void stop() {
        if (subscription != null) {
            subscription.dispose();
        }
    }

    //never fails: the next press still has to be handled
    Mono<Void> press() {
        return Mono.defer(() -> {
            final Observation observation = Observation.createNotStarted(OBSERVATION, observationRegistry).start();
            return Mono.fromRunnable(() -> log.info("The gate button of the entrance panel pressed"))
                .then(Mono.defer(gateClient::open))
                .flatMap(this::tell)
                .onErrorResume(failure -> {
                    log.error("A press of the gate button of the entrance panel not handled", failure);
                    return Mono.empty();
                })
                .doFinally(signal -> observation.stop())
                .contextWrite(context -> context.put(ObservationThreadLocalAccessor.KEY, observation));
        });
    }

    private Mono<Void> tell(final boolean opening) {
        return panel.gateResult(opening)
            .doOnSuccess(nothing -> log.info("The entrance panel told the gate {}", opening ? "is opening" : "did not open"))
            .onErrorResume(failure -> {
                log.warn("The entrance panel not told the gate {}: {}", opening ? "is opening" : "did not open",
                    failure.getMessage());
                return Mono.empty();
            });
    }
}

package cloud.cholewa.amx.gate;

import cloud.cholewa.commons.error.DownstreamErrors;
import cloud.cholewa.commons.error.model.DownstreamError;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;

import java.util.concurrent.TimeoutException;

/**
 * The driveway gate opened for the entrance panel (HAS-487): {@code POST /home/gate/open?source=panel}
 * of gate-service (>= 0.11.0, HAS-483), over k8s DNS.
 * <p>
 * Answers whether the gate is opening, never an error: a 2xx, and {@code 409 OPEN_TOO_SOON} - an
 * opening of the last seconds, the gate is moving already. Anything else - a 3xx included - is not
 * opening, a WARN with the status alone (or the kind of failure: gate-service away; no answer within
 * the timeout, worded as a gate that may have opened all the same).
 * <b>Sent once and never repeated</b>: a call cut off may still have opened the gate, and a second
 * pulse of its Shelly is gate-service's to refuse, not this client's to risk.
 */
@Slf4j
@Component
public class GateClient {

    static final String OPEN = "/home/gate/open";
    static final String SOURCE = "panel";
    //the name of a GateError constant of gate-service - wire contract, pinned there
    static final String OPEN_TOO_SOON = "OPEN_TOO_SOON";

    private final WebClient webClient;
    private final GateServiceProperties properties;

    public GateClient(final WebClient webClient, final GateServiceProperties properties) {
        this.webClient = webClient;
        this.properties = properties;
    }

    /** {@code true} when the gate is opening; never fails. */
    public Mono<Boolean> open() {
        return Mono.defer(() -> webClient.post()
                .uri(uriBuilder -> properties.base(uriBuilder).path(OPEN).queryParam("source", SOURCE).build())
                .retrieve()
                //a redirect is no opening either: only a 2xx says the Shelly took the pulse
                .onStatus(status -> !status.is2xxSuccessful(), response -> DownstreamErrors.read(response).map(GateRefused::new))
                .toBodilessEntity()
                .timeout(properties.responseTimeout()))
            .doOnNext(answer -> log.info("The gate opened for the entrance panel ({})", answer.getStatusCode().value()))
            .thenReturn(true)
            .onErrorResume(GateRefused.class, refused -> Mono.fromSupplier(() -> answered(refused.error())))
            .onErrorResume(TimeoutException.class, timeout -> {
                log.warn("No answer of gate-service within {} - the gate may have opened all the same; the entrance panel"
                    + " is told it did not", properties.responseTimeout());
                return Mono.just(false);
            })
            .onErrorResume(failure -> !(failure instanceof GateRefused), failure -> {
                log.warn("The gate not opened for the entrance panel: {}", failure.getClass().getSimpleName());
                return Mono.just(false);
            });
    }

    //whether the gate is opening all the same: an opening of the last seconds is refused as too soon
    private static boolean answered(final DownstreamError error) {
        if (error.status().value() == HttpStatus.CONFLICT.value() && error.hasCode(OPEN_TOO_SOON)) {
            log.info("The gate is opening already for the entrance panel (409 {})", OPEN_TOO_SOON);
            return true;
        }
        log.warn("The gate not opened for the entrance panel: gate-service answered {}", error.status().value());
        return false;
    }

    //what gate-service answered; its body goes nowhere, only the status and the code are read
    private static final class GateRefused extends RuntimeException {

        private final transient DownstreamError error;

        private GateRefused(final DownstreamError error) {
            super("gate-service answered " + error.status().value(), null, false, false);
            this.error = error;
        }

        private DownstreamError error() {
            return error;
        }
    }
}

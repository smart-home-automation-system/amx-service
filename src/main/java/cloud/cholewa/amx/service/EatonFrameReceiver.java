package cloud.cholewa.amx.service;

import cloud.cholewa.amx.frameserver.FrameServerClient;
import cloud.cholewa.amx.frameserver.ReceivedFrame;
import cloud.cholewa.amx.infrastructure.error.ConfigurationCallException;
import cloud.cholewa.amx.infrastructure.error.EatonParsingException;
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
 * Takes every frame the frame server client receives and runs it through {@link AmxService},
 * one after the other, for as long as the application runs. Nothing a frame does can end the
 * subscription: every failure is a line in the log and the next frame is taken.
 * <p>
 * The level of that line is what the alerts see. A frame the parser refuses, or a data point
 * {@code database-service} knows no device for, is the gateway's or the registry's business and
 * a WARN; a {@code database-service} that fails or does not answer, and anything unexpected, is
 * an ERROR. What a failing {@code database-service} said is logged here and nowhere else - the
 * frames no longer come as HTTP requests, so there is no answer it could leak into.
 * <p>
 * A frame has no request to take a trace from, so each one starts an observation of its own:
 * the lines logged while it is processed share one {@code traceId}, as those of a request do.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class EatonFrameReceiver {

    static final String OBSERVATION = "eaton.frame";

    private final FrameServerClient frameServerClient;
    private final AmxService amxService;
    private final ObservationRegistry observationRegistry;

    private Disposable subscription;

    @PostConstruct
    void subscribe() {
        subscription = frameServerClient.frames()
            .concatMap(this::handle, 1)
            .subscribe(
                nothing -> { },
                failure -> log.error("The receiver of Eaton frames ended: no frame is processed any more", failure),
                () -> log.info("The receiver of Eaton frames ended with the client")
            );
    }

    @PreDestroy
    void unsubscribe() {
        if (subscription != null) {
            subscription.dispose();
        }
    }

    //never fails: not for what the pipeline does, and not for what the explaining of it does
    Mono<Void> handle(final ReceivedFrame received) {
        return Mono.defer(() -> {
                final Observation observation = Observation.createNotStarted(OBSERVATION, observationRegistry)
                    .lowCardinalityKeyValue("gateway", received.gateway().getValue())
                    .start();
                //logged inside the chain, where the observation is the trace of the line, and before
                //the pipeline is subscribed to - its first lines are written on the subscribe itself
                return Mono.fromRunnable(() -> log.info(
                        "Received frame from gateway {}: [{}]", received.gateway(), received.frame()))
                    .then(Mono.defer(() -> amxService.consume(received)))
                    .onErrorResume(failure -> {
                        explain(received, failure);
                        return Mono.empty();
                    })
                    .doFinally(signal -> observation.stop())
                    .contextWrite(context -> context.put(ObservationThreadLocalAccessor.KEY, observation));
            })
            .onErrorResume(failure -> {
                log.error("Frame [{}] from gateway {} not processed, and the failure could not be explained",
                    received.frame(), received.gateway(), failure);
                return Mono.empty();
            });
    }

    private static void explain(final ReceivedFrame received, final Throwable failure) {
        if (failure instanceof EatonParsingException) {
            log.warn("Frame [{}] from gateway {} rejected: {}", received.frame(), received.gateway(), failure.getMessage());
        } else if (failure instanceof ConfigurationCallException lookup) {
            if (lookup.getHttpStatus() != null && lookup.getHttpStatus().is4xxClientError()) {
                log.warn(
                    "Frame [{}] from gateway {} not processed: {}",
                    received.frame(), received.gateway(), lookup.describe()
                );
            } else {
                log.error(
                    "Frame [{}] from gateway {} not processed: {}",
                    received.frame(), received.gateway(), lookup.describe()
                );
            }
        } else {
            log.error("Frame [{}] from gateway {} not processed", received.frame(), received.gateway(), failure);
        }
    }
}

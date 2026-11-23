package cloud.cholewa.amx.panelserver;

import cloud.cholewa.amx.weather.OutsideTemperatureClient;
import cloud.cholewa.commons.scheduling.ScheduledPass;
import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import reactor.core.Disposable;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicReference;

/**
 * The outside temperature on the entrance panel (HAS-486, epic HAS-479): every minute the
 * temperature of the weather station is read from weather-service and sent to the panel server as
 * {@code temp;<value>} - one decimal, or {@code temp;} for "not known" (a stale reading, or a read
 * that failed: never a zero) - only when it is not what the panel was last told on this
 * connection. The master forgets the value when its client goes away, so every new connection is
 * told the last value read once it held for {@link #SETTLE} - not a minute later, and not at once:
 * a server that takes the connection and never answers would otherwise be asked, timed out and
 * reconnected every few seconds (the keepalive asks it once a minute).
 * <p>
 * A panel server that is not connected is not asked at all (the next connection is told); a
 * command it does not take is a WARN, and the value is sent again with the next pass. The state is
 * in memory: what was read last, and what the master took on which connection.
 */
@Slf4j
@Component
public class PanelTemperature {

    //below the minute between passes; a read is bounded at 5 s, a command at the response timeout
    static final Duration LIMIT = Duration.ofSeconds(50);
    //a new connection is told the temperature once it held this long, still on the same connection
    static final Duration SETTLE = Duration.ofSeconds(10);

    private final OutsideTemperatureClient outsideTemperature;
    private final PanelServerClient panel;
    private final Disposable reconnects;

    //null until the first read
    private final AtomicReference<String> latest = new AtomicReference<>();
    //what the master took last, and on which connection
    private final AtomicReference<Shown> shown = new AtomicReference<>(new Shown(null, 0));
    //what the master did not take last: the same again is a DEBUG, not a WARN a minute
    private final AtomicReference<Shown> failed = new AtomicReference<>();

    @Autowired
    public PanelTemperature(final OutsideTemperatureClient outsideTemperature, final PanelServerClient panel) {
        this(outsideTemperature, panel, SETTLE);
    }

    PanelTemperature(final OutsideTemperatureClient outsideTemperature, final PanelServerClient panel, final Duration settle) {
        this.outsideTemperature = outsideTemperature;
        this.panel = panel;
        this.reconnects = panel.connections()
            .flatMap(connection -> Mono.delay(settle)
                .filter(tick -> panel.isConnected() && panel.connectionNumber() == connection)
                .flatMap(tick -> Mono.justOrEmpty(latest.get()))
                .flatMap(value -> show(value, connection)))
            .subscribe(nothing -> { }, failure -> log.error(
                "The entrance panel is no longer told the temperature on a new connection", failure));
    }

    @Scheduled(initialDelayString = "PT15S", fixedDelayString = "PT1M")
    Mono<Void> refresh() {
        return ScheduledPass.bounded("Entrance panel temperature", LIMIT, this::pass);
    }

    Mono<Void> pass() {
        return outsideTemperature.read()
            .doOnNext(latest::set)
            //the connection is read when the value is there, never when the chain is built
            .flatMap(value -> Mono.just(panel.connectionNumber())
                .filter(connection -> panel.isConnected())
                .filter(connection -> !new Shown(value, connection).equals(shown.get()))
                .flatMap(connection -> show(value, connection)));
    }

    @PreDestroy
    void stop() {
        reconnects.dispose();
    }

    //never fails: a value the master did not take is sent again with the next pass
    private Mono<Void> show(final String value, final long connection) {
        return panel.temperature(value)
            .doOnSuccess(nothing -> {
                shown.set(new Shown(value, connection));
                failed.set(null);
                log.info("The entrance panel shows the outside temperature: {}",
                    value.isEmpty() ? "not known" : value + " C");
            })
            .onErrorResume(failure -> {
                final Shown attempt = new Shown(value, connection);
                if (attempt.equals(failed.getAndSet(attempt))) {
                    log.debug("The entrance panel still not told the outside temperature [{}]: {}", value, failure.getMessage());
                } else {
                    log.warn("The entrance panel not told the outside temperature [{}], tried again with the next pass: {}",
                        value, failure.getMessage());
                }
                return Mono.empty();
            });
    }

    private record Shown(String value, long connection) {
    }
}

package cloud.cholewa.amx.weather;

import lombok.extern.slf4j.Slf4j;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.HttpStatusCode;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * The outside temperature measured by the weather station (HAS-486): {@code GET /home/weather/current}
 * of weather-service, the metric {@code temperature} with its {@code value} and {@code stale}.
 * <p>
 * What the panel is to show, never an error: one decimal with a point ({@code -3.4}), or the empty
 * text for "not known" - a stale reading, a station without a temperature, and every failed call.
 * No body, or JSON that is not the model ({@code {}} decodes into a map without the metric, a
 * metric without a value into nulls), is a failed call: never a zero on the panel. A failed call
 * is a WARN when the calls start failing and a DEBUG while they keep failing, so weather-service
 * away for an hour is one line a minute only at DEBUG.
 */
@Slf4j
@Component
public class OutsideTemperatureClient {

    static final String CURRENT = "/home/weather/current";
    static final String METRIC = "temperature";
    static final String NOT_KNOWN = "";

    //the whole answer as plain JSON: only the temperature is read, so no other metric can fail it
    private static final ParameterizedTypeReference<Map<String, Object>> CURRENT_WEATHER =
        new ParameterizedTypeReference<>() { };

    private final WebClient webClient;
    private final WeatherServiceProperties properties;
    private final AtomicBoolean failing = new AtomicBoolean();

    public OutsideTemperatureClient(final WebClient webClient, final WeatherServiceProperties properties) {
        this.webClient = webClient;
        this.properties = properties;
    }

    /** One decimal with a point, or {@link #NOT_KNOWN}; never fails. */
    public Mono<String> read() {
        return Mono.defer(() -> webClient.get()
                .uri(uriBuilder -> properties.base(uriBuilder).path(CURRENT).build())
                .retrieve()
                .onStatus(HttpStatusCode::isError, response -> response.releaseBody()
                    .then(Mono.error(new IllegalStateException("weather-service answered " + response.statusCode().value()))))
                .bodyToMono(CURRENT_WEATHER)
                .switchIfEmpty(Mono.error(() -> new IllegalStateException("weather-service answered without a body")))
                .timeout(properties.responseTimeout()))
            .map(OutsideTemperatureClient::shown)
            .doOnNext(value -> {
                if (failing.compareAndSet(true, false)) {
                    log.info("The outside temperature is read from weather-service again");
                }
            })
            .onErrorResume(failure -> {
                if (failing.compareAndSet(false, true)) {
                    log.warn("The outside temperature not read from weather-service, the panel shows it as not known: {}",
                        failure.toString());
                } else {
                    log.debug("The outside temperature still not read from weather-service: {}", failure.toString());
                }
                return Mono.just(NOT_KNOWN);
            });
    }

    //a stale reading is not known; a temperature that is not the model is a failed call
    private static String shown(final Map<String, Object> current) {
        final CurrentMetric temperature = Optional.ofNullable(current.get(METRIC))
            .filter(Map.class::isInstance)
            .map(metric -> (Map<?, ?>) metric)
            .map(metric -> new CurrentMetric(metric.get("value"), metric.get("stale")))
            .filter(metric -> metric.value() instanceof Number && metric.stale() instanceof Boolean)
            .orElseThrow(() -> new IllegalStateException("weather-service answered no temperature: " + current.keySet()));
        return Boolean.TRUE.equals(temperature.stale())
            ? NOT_KNOWN
            //through its text: a double read as it was written (0.8, not 0.8000000000000000444)
            : new BigDecimal(temperature.value().toString()).setScale(1, RoundingMode.HALF_UP).toPlainString();
    }

    /**
     * The temperature of {@code /current}, as much of it as the panel needs. Read out of the plain
     * JSON of the whole answer: the other metrics hold anything ({@code raining} is a boolean), and a
     * model of every metric failed the whole answer on the first read in the cluster (2026-11-23).
     */
    private record CurrentMetric(Object value, Object stale) {
    }
}

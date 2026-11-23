package cloud.cholewa.amx.weather;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import org.hibernate.validator.constraints.time.DurationMax;
import org.hibernate.validator.constraints.time.DurationMin;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;
import org.springframework.boot.convert.DurationUnit;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.util.UriBuilder;

import java.time.Duration;
import java.time.temporal.ChronoUnit;

/**
 * weather-service over k8s DNS (HAS-486): the outside temperature of the entrance panel.<br>
 * {@code response-timeout} - a bare number is seconds; well below the minute between the reads.
 */
@Validated
@ConfigurationProperties("internal.service.weather")
public record WeatherServiceProperties(
    @NotBlank String host,
    @NotNull Integer port,
    @NotNull @DurationMin(seconds = 1) @DurationMax(seconds = 20) @DurationUnit(ChronoUnit.SECONDS)
    @DefaultValue("5s") Duration responseTimeout
) {
    public UriBuilder base(final UriBuilder uriBuilder) {
        return uriBuilder.scheme("http").host(host).port(port);
    }
}

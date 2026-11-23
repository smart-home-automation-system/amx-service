package cloud.cholewa.amx.gate;

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
 * gate-service over k8s DNS (HAS-487): the gate opened when the entrance panel asks.<br>
 * {@code response-timeout} - a bare number is seconds; gate-service answers once the Shelly of the
 * gate took the pulse, in well under a second.
 */
@Validated
@ConfigurationProperties("internal.service.gate")
public record GateServiceProperties(
    @NotBlank String host,
    @NotNull Integer port,
    @NotNull @DurationMin(seconds = 1) @DurationMax(seconds = 20) @DurationUnit(ChronoUnit.SECONDS)
    @DefaultValue("10s") Duration responseTimeout
) {
    public UriBuilder base(final UriBuilder uriBuilder) {
        return uriBuilder.scheme("http").host(host).port(port);
    }
}

package cloud.cholewa.amx.config;

import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import org.hibernate.validator.constraints.time.DurationMax;
import org.hibernate.validator.constraints.time.DurationMin;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;
import org.springframework.boot.convert.DurationUnit;
import org.springframework.validation.annotation.Validated;

import java.time.Duration;
import java.time.temporal.ChronoUnit;

/**
 * The TCP server of the AMX master that moves Eaton frames between this service and the serial
 * gateways, over one permanent connection.<br>
 * {@code host} - mandatory and without a default in the document a pod runs with: a default
 * would let the pod start, turn Ready, receive nothing and fail every command.<br>
 * {@code connectTimeout} - how long one attempt to connect may take; the attempts go on, with a
 * backoff, until one succeeds.<br>
 * {@code responseTimeout} - how long the server may take to answer a frame it was sent.<br>
 * A bare number is seconds: without the unit it would bind as milliseconds and every command
 * would time out. The upper bounds catch a value meant as milliseconds ("3000") and keep the
 * wait for an answer well below the 30 s after which the gateway answers its caller a 504.
 */
@Validated
@ConfigurationProperties(prefix = "amx.frame-server")
public record FrameServerProperties(
    @NotBlank String host,
    @NotNull @Min(1) @Max(65535) Integer port,
    @NotNull @DurationMin(seconds = 1) @DurationMax(seconds = 5) @DurationUnit(ChronoUnit.SECONDS)
    @DefaultValue("2s") Duration connectTimeout,
    @NotNull @DurationMin(seconds = 1) @DurationMax(seconds = 10) @DurationUnit(ChronoUnit.SECONDS)
    @DefaultValue("3s") Duration responseTimeout
) {
}

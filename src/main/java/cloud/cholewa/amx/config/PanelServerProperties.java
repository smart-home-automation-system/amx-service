package cloud.cholewa.amx.config;

import cloud.cholewa.amx.lineserver.LineServerEndpoint;
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
 * The panel server of the Office master (HAS-484): a TCP server in NetLinx that drives the
 * entrance panel at the door, over one permanent connection.<br>
 * {@code host} - mandatory and without a default in the document a pod runs with (manifest
 * {@code AMX_PANEL_SERVER_HOST}): a default would let the pod start and refuse every ring.<br>
 * {@code port} - 20011, beside the 20010 of the frame server of the Television master.<br>
 * The timeouts have the bounds of the frame server, for the same reasons: a bare number is
 * seconds, and the wait for an answer stays well below the 30 s after which the gateway answers
 * its caller a 504.
 */
@Validated
@ConfigurationProperties(prefix = "amx.panel-server")
public record PanelServerProperties(
    @NotBlank String host,
    @NotNull @Min(1) @Max(65535) @DefaultValue("20011") Integer port,
    @NotNull @DurationMin(seconds = 1) @DurationMax(seconds = 5) @DurationUnit(ChronoUnit.SECONDS)
    @DefaultValue("2s") Duration connectTimeout,
    @NotNull @DurationMin(seconds = 1) @DurationMax(seconds = 10) @DurationUnit(ChronoUnit.SECONDS)
    @DefaultValue("3s") Duration responseTimeout
) implements LineServerEndpoint {
}

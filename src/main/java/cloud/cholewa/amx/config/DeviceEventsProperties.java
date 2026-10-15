package cloud.cholewa.amx.config;

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
 * The second connection to the broker, the one the events of the Eaton devices are published
 * over, and where they go. Host, port, connection timeout and heartbeat are those of
 * {@code spring.rabbitmq.*} - it is the same broker, only another virtual host.<br>
 * {@code password} - mandatory and without a default in the document a pod runs with
 * ({@code DEVICE_EVENTS_PASSWORD} in the manifest): with one the pod would start, turn Ready and
 * fail every publish.<br>
 * {@code exchange} - the topic exchange every client of {@code /device} publishes to, MQTT
 * included; the queues of the device services are bound to it with {@code #}.<br>
 * {@code confirmTimeout} - how long the confirmation of the broker is waited for. The frames are
 * processed one at a time, so this is also how long one broker that does not answer holds up the
 * next frame. A bare number is seconds.
 */
@Validated
@ConfigurationProperties(prefix = "device-events")
public record DeviceEventsProperties(
    @NotBlank String virtualHost,
    @NotBlank String username,
    @NotBlank String password,
    @NotBlank String exchange,
    @NotNull @DurationMin(seconds = 1) @DurationMax(seconds = 10) @DurationUnit(ChronoUnit.SECONDS)
    @DefaultValue("5s") Duration confirmTimeout
) {

    //a record prints its components, and one of them is the password
    @Override
    public String toString() {
        return "DeviceEventsProperties[virtualHost=" + virtualHost + ", username=" + username
            + ", exchange=" + exchange + ", confirmTimeout=" + confirmTimeout + "]";
    }
}

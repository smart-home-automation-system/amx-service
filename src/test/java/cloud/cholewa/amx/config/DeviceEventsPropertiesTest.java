package cloud.cholewa.amx.config;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Configuration;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

class DeviceEventsPropertiesTest {

    private final ApplicationContextRunner runner = new ApplicationContextRunner()
        .withUserConfiguration(Config.class)
        .withPropertyValues(
            "device-events.virtual-host=/device",
            "device-events.username=device",
            "device-events.password=secret",
            "device-events.exchange=amq.topic"
        );

    @Test
    void should_bind_the_connection_and_default_the_confirm_timeout() {
        runner.run(context -> {
            assertThat(context).hasNotFailed();
            assertThat(context.getBean(DeviceEventsProperties.class)).isEqualTo(new DeviceEventsProperties(
                "/device", "device", "secret", "amq.topic", Duration.ofSeconds(5)
            ));
        });
    }

    @Test
    void should_not_print_the_password() {
        runner.run(context -> assertThat(context.getBean(DeviceEventsProperties.class).toString())
            .doesNotContain("secret")
            .contains("/device", "device", "amq.topic"));
    }

    //without the unit "3" would be three milliseconds and every publish a timeout
    @Test
    void should_read_a_bare_number_as_seconds() {
        runner.withPropertyValues("device-events.confirm-timeout=3")
            .run(context -> assertThat(context.getBean(DeviceEventsProperties.class).confirmTimeout())
                .isEqualTo(Duration.ofSeconds(3)));
    }

    //a pod without the password must not start, turn Ready and fail every publish
    @Test
    void should_not_start_when_the_password_is_not_set_at_all() {
        new ApplicationContextRunner()
            .withUserConfiguration(Config.class)
            .withPropertyValues(
                "device-events.virtual-host=/device",
                "device-events.username=device",
                "device-events.exchange=amq.topic"
            )
            .run(context -> assertThat(context).hasFailed());
    }

    @ParameterizedTest
    @ValueSource(strings = {
        "device-events.password=",
        "device-events.virtual-host= ",
        "device-events.username=",
        "device-events.exchange=",
        //zero or milliseconds written where seconds are read
        "device-events.confirm-timeout=0",
        "device-events.confirm-timeout=5000",
        //the frames wait for it one after the other
        "device-events.confirm-timeout=11s"
    })
    void should_not_start_with_a_value_out_of_bounds(final String property) {
        runner.withPropertyValues(property).run(context -> assertThat(context).hasFailed());
    }

    @Configuration
    @EnableConfigurationProperties(DeviceEventsProperties.class)
    static class Config {
    }
}

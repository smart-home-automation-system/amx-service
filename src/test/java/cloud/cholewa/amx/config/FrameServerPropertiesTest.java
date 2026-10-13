package cloud.cholewa.amx.config;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Configuration;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

class FrameServerPropertiesTest {

    private final ApplicationContextRunner runner = new ApplicationContextRunner()
        .withUserConfiguration(Config.class)
        .withPropertyValues("amx.frame-server.host=amx-master", "amx.frame-server.port=20010");

    @Test
    void should_bind_the_address_and_default_the_timeouts() {
        runner.run(context -> {
            assertThat(context).hasNotFailed();
            assertThat(context.getBean(FrameServerProperties.class)).isEqualTo(new FrameServerProperties(
                "amx-master", 20010, Duration.ofSeconds(2), Duration.ofSeconds(3)
            ));
        });
    }

    //without the unit "4" would be four milliseconds and every command a timeout
    @Test
    void should_read_a_bare_number_as_seconds() {
        runner
            .withPropertyValues("amx.frame-server.connect-timeout=4", "amx.frame-server.response-timeout=5")
            .run(context -> {
                final FrameServerProperties properties = context.getBean(FrameServerProperties.class);
                assertThat(properties.connectTimeout()).isEqualTo(Duration.ofSeconds(4));
                assertThat(properties.responseTimeout()).isEqualTo(Duration.ofSeconds(5));
            });
    }

    //a pod without the address of the master must not start, turn Ready and fail every command
    @ParameterizedTest
    @ValueSource(strings = {"amx.frame-server.host=", "amx.frame-server.host=  "})
    void should_not_start_without_a_host(final String host) {
        runner.withPropertyValues(host).run(context -> assertThat(context).hasFailed());
    }

    @Test
    void should_not_start_when_the_host_is_not_set_at_all() {
        new ApplicationContextRunner()
            .withUserConfiguration(Config.class)
            .withPropertyValues("amx.frame-server.port=20010")
            .run(context -> assertThat(context).hasFailed());
    }

    @ParameterizedTest
    @ValueSource(strings = {
        "amx.frame-server.port=0",
        "amx.frame-server.port=65536",
        //zero switches a timeout off
        "amx.frame-server.connect-timeout=0",
        "amx.frame-server.response-timeout=0",
        //milliseconds written where seconds are read
        "amx.frame-server.connect-timeout=2000",
        "amx.frame-server.response-timeout=3000",
        "amx.frame-server.response-timeout=11s"
    })
    void should_not_start_with_a_value_out_of_bounds(final String property) {
        runner.withPropertyValues(property).run(context -> assertThat(context).hasFailed());
    }

    @Configuration
    @EnableConfigurationProperties(FrameServerProperties.class)
    static class Config {
    }
}

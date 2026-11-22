package cloud.cholewa.amx.config;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Configuration;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

class PanelServerPropertiesTest {

    private final ApplicationContextRunner runner = new ApplicationContextRunner()
        .withUserConfiguration(Config.class)
        .withPropertyValues("amx.panel-server.host=office-master");

    @Test
    void should_bind_the_host_and_default_the_port_and_the_timeouts() {
        runner.run(context -> {
            assertThat(context).hasNotFailed();
            assertThat(context.getBean(PanelServerProperties.class)).isEqualTo(new PanelServerProperties(
                "office-master", 20011, Duration.ofSeconds(2), Duration.ofSeconds(3)
            ));
        });
    }

    @Test
    void should_read_a_bare_number_as_seconds() {
        runner
            .withPropertyValues("amx.panel-server.connect-timeout=4", "amx.panel-server.response-timeout=5")
            .run(context -> {
                final PanelServerProperties properties = context.getBean(PanelServerProperties.class);
                assertThat(properties.connectTimeout()).isEqualTo(Duration.ofSeconds(4));
                assertThat(properties.responseTimeout()).isEqualTo(Duration.ofSeconds(5));
            });
    }

    //a pod without the address of the Office master must not start and refuse every ring
    @ParameterizedTest
    @ValueSource(strings = {"amx.panel-server.host=", "amx.panel-server.host=  "})
    void should_not_start_without_a_host(final String host) {
        runner.withPropertyValues(host).run(context -> assertThat(context).hasFailed());
    }

    @Test
    void should_not_start_when_the_host_is_not_set_at_all() {
        new ApplicationContextRunner()
            .withUserConfiguration(Config.class)
            .run(context -> assertThat(context).hasFailed());
    }

    @ParameterizedTest
    @ValueSource(strings = {
        "amx.panel-server.port=0",
        "amx.panel-server.port=65536",
        "amx.panel-server.connect-timeout=0",
        "amx.panel-server.response-timeout=0",
        "amx.panel-server.connect-timeout=2000",
        "amx.panel-server.response-timeout=3000",
        "amx.panel-server.connect-timeout=6s",
        "amx.panel-server.response-timeout=11s"
    })
    void should_not_start_with_a_value_out_of_bounds(final String property) {
        runner.withPropertyValues(property).run(context -> assertThat(context).hasFailed());
    }

    @Configuration
    @EnableConfigurationProperties(PanelServerProperties.class)
    static class Config {
    }
}

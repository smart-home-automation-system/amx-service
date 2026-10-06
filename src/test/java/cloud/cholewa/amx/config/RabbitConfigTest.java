package cloud.cholewa.amx.config;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.connection.ConnectionNameStrategy;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

class RabbitConfigTest {

    private static final String SERVICE = "amx-service";

    //the properties are set here, above the environment of the machine, so the test reads the
    //same on a workstation, in CI and in a container
    private final ApplicationContextRunner contextRunner = new ApplicationContextRunner()
        .withUserConfiguration(RabbitConfig.class)
        .withPropertyValues("spring.application.name=" + SERVICE);

    @ParameterizedTest(name = "HOSTNAME [{0}] gives the connection name {1}")
    @CsvSource({
        //a pod of this service: its name, which tells the old pod from the new one in a rollout
        SERVICE + "-5968757896-bghz5, " + SERVICE + "-5968757896-bghz5",
        //a workstation, a container id, a pod of another service, an empty value
        "DESKTOP-4F2K, " + SERVICE + "-local",
        "3f2a9c1b7d4e, " + SERVICE + "-local",
        "other-service-5968757896-bghz5, " + SERVICE + "-local",
        SERVICE + ", " + SERVICE + "-local",
        "'', " + SERVICE + "-local"
    })
    void should_name_the_connection_after_the_pod_and_nothing_else(final String hostname, final String expected) {
        contextRunner
            .withPropertyValues("HOSTNAME=" + hostname)
            .run(context -> assertThat(connectionName(context.getBean(ConnectionNameStrategy.class)))
                .isEqualTo(expected));
    }

    private static String connectionName(final ConnectionNameStrategy strategy) {
        return strategy.obtainNewConnectionName(mock(ConnectionFactory.class));
    }
}

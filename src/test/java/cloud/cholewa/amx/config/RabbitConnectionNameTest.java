package cloud.cholewa.amx.config;

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.connection.ConnectionNameStrategy;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.ApplicationContext;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.util.ReflectionTestUtils;

import static org.assertj.core.api.Assertions.assertThat;

//the name the broker shows for the connection is not made in this service any more: the strategy
//comes from cholewa-commons (RabbitConnectionNameAutoConfiguration, since 1.8.0). What stays here
//is the proof that the whole application still gets it - a ConnectionNameStrategy bean added to
//this service by accident makes the library back off, and a library that changes its rule or its
//bean changes what the management UI shows during a rollout.
//HOSTNAME is set by the test, above the environment of the machine, so it reads the same on a
//workstation, in CI and in a container. Nothing here opens a connection: the factory is asked for
//its strategy, never for a connection
class RabbitConnectionNameTest {

    private static final String LIBRARY_BEAN = "cholewaPodConnectionNameStrategy";
    private static final String POD = "amx-service-5968757896-bghz5";

    @Nested
    @SpringBootTest(properties = "HOSTNAME=" + POD)
    @ActiveProfiles("test")
    class InAPod {

        @Autowired
        private ApplicationContext context;

        @Autowired
        private CachingConnectionFactory connectionFactory;

        @Test
        void should_take_the_strategy_from_the_library_and_no_other() {
            assertThat(context.getBeansOfType(ConnectionNameStrategy.class)).containsOnlyKeys(LIBRARY_BEAN);
        }

        @Test
        void should_name_the_connection_of_the_auto_configured_factory_after_the_pod() {
            assertThat(context.getBeansOfType(ConnectionFactory.class)).hasSize(1);
            assertThat(strategyOf(connectionFactory)).isSameAs(context.getBean(LIBRARY_BEAN));
            assertThat(strategyOf(connectionFactory).obtainNewConnectionName(connectionFactory)).isEqualTo(POD);
        }
    }

    @Nested
    @SpringBootTest(properties = "HOSTNAME=DESKTOP-4F2K")
    @ActiveProfiles("test")
    class OutsideTheCluster {

        @Autowired
        private CachingConnectionFactory connectionFactory;

        @Test
        void should_name_the_connection_local() {
            assertThat(strategyOf(connectionFactory).obtainNewConnectionName(connectionFactory))
                .isEqualTo("amx-service-local");
        }
    }

    //there is no getter for the strategy of a connection factory, hence the reflection
    private static ConnectionNameStrategy strategyOf(final CachingConnectionFactory connectionFactory) {
        return (ConnectionNameStrategy) ReflectionTestUtils.getField(connectionFactory, "connectionNameStrategy");
    }
}

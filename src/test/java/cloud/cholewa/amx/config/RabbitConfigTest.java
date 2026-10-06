package cloud.cholewa.amx.config;

import org.junit.jupiter.api.Test;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

class RabbitConfigTest {

    //the pod name alone: it used to be decorated with the service name a second time, or
    //replaced by something that told no pod from another
    @Test
    void should_name_the_connection_after_the_pod() {
        final String name = new RabbitConfig()
            .connectionNameStrategy("amx-service-5968757896-bghz5")
            .obtainNewConnectionName(mock(ConnectionFactory.class));

        assertThat(name).isEqualTo("amx-service-5968757896-bghz5");
    }
}

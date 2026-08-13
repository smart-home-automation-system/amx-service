package cloud.cholewa.amx;

import org.junit.jupiter.api.Test;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.support.converter.JacksonJsonMessageConverter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.util.ReflectionTestUtils;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest
class AmxServiceApplicationTest {

    @Autowired
    private RabbitTemplate rabbitTemplate;

    @Test
    void contextLoads() {
    }

    //guards the reason RabbitConfig declares no RabbitTemplate: spring.rabbitmq.template.* is
    //applied by RabbitTemplateConfigurer only, and a hand-built template would silently publish
    //without a traceparent header again. There is no getter for the flag, hence the reflection
    @Test
    void should_use_the_auto_configured_template_with_observations_enabled() {
        assertThat(ReflectionTestUtils.getField(rabbitTemplate, "observationEnabled")).isEqualTo(true);
        assertThat(rabbitTemplate.getMessageConverter()).isInstanceOf(JacksonJsonMessageConverter.class);
    }
}

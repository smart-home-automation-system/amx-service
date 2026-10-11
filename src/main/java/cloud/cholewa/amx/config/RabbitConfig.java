package cloud.cholewa.amx.config;

import org.springframework.amqp.core.ExchangeBuilder;
import org.springframework.amqp.core.FanoutExchange;
import org.springframework.amqp.support.converter.JacksonJsonMessageConverter;
import org.springframework.amqp.support.converter.MessageConverter;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

//the RabbitTemplate is deliberately left to the auto-configuration: only the template built by
//RabbitTemplateConfigurer gets spring.rabbitmq.template.* applied, and RabbitAutoConfiguration
//backs off from any RabbitOperations bean. A hand-built one silently ignored both
//observation-enabled (no traceparent on the published message, so the trace stopped at the queue)
//and the retry settings. The converter below is picked up by the configurer.
//The name of the connection - the pod - comes from cholewa-commons
//(RabbitConnectionNameAutoConfiguration), which backs off from a ConnectionNameStrategy declared
//here
@Configuration
public class RabbitConfig {

    @Bean
    MessageConverter messageConverter() {
        return new JacksonJsonMessageConverter();
    }

    @Bean
    FanoutExchange temperatureExchange() {
        return ExchangeBuilder
            .fanoutExchange("temperature.events")
            .durable(true)
            .build();
    }
}

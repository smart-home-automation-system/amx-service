package cloud.cholewa.amx.config;

import org.springframework.amqp.core.ExchangeBuilder;
import org.springframework.amqp.core.FanoutExchange;
import org.springframework.amqp.rabbit.connection.ConnectionNameStrategy;
import org.springframework.amqp.support.converter.JacksonJsonMessageConverter;
import org.springframework.amqp.support.converter.MessageConverter;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

//the RabbitTemplate is deliberately left to the auto-configuration: only the template built by
//RabbitTemplateConfigurer gets spring.rabbitmq.template.* applied, and RabbitAutoConfiguration
//backs off from any RabbitOperations bean. A hand-built one silently ignored both
//observation-enabled (no traceparent on the published message, so the trace stopped at the queue)
//and the retry settings. The converter below is picked up by the configurer
@Configuration
public class RabbitConfig {

    @Bean
    MessageConverter messageConverter() {
        return new JacksonJsonMessageConverter();
    }

    //the name the broker shows for the connection: the pod, which already says which service it
    //is and tells the old pod from the new one during a rollout. Kubernetes sets HOSTNAME;
    //outside the cluster there is none
    @Bean
    ConnectionNameStrategy connectionNameStrategy(
        @Value("${HOSTNAME:amx-service-local}") final String hostname
    ) {
        return connectionFactory -> hostname;
    }

    @Bean
    FanoutExchange temperatureExchange() {
        return ExchangeBuilder
            .fanoutExchange("temperature.events")
            .durable(true)
            .build();
    }
}

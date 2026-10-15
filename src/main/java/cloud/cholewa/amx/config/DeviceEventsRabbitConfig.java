package cloud.cholewa.amx.config;

import cloud.cholewa.amx.rabbit.DeviceEventConnection;
import cloud.cholewa.amx.rabbit.DeviceEventPublisher;
import org.springframework.amqp.rabbit.connection.ConnectionNameStrategy;
import org.springframework.amqp.support.converter.MessageConverter;
import org.springframework.boot.amqp.autoconfigure.RabbitProperties;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

//the events of the Eaton devices go to the virtual host /device, and a connection belongs to
//exactly one, so publishing there takes a second connection. DeviceEventConnection keeps its
//factory and its template to itself - see there why neither may be a bean
@Configuration
public class DeviceEventsRabbitConfig {

    //the strategy is the one that names the connection of spring.rabbitmq.*; this one gets the
    //same name with "/device" added. Closed with the context: Spring finds close() of an
    //AutoCloseable bean by itself
    @Bean
    DeviceEventConnection deviceEventConnection(
        final RabbitProperties rabbitProperties,
        final DeviceEventsProperties properties,
        final ConnectionNameStrategy connectionNameStrategy,
        final MessageConverter messageConverter,
        final ApplicationContext applicationContext
    ) {
        return DeviceEventConnection.create(
            rabbitProperties,
            properties.virtualHost(),
            properties.username(),
            properties.password(),
            connectionNameStrategy,
            messageConverter,
            applicationContext
        );
    }

    @Bean
    DeviceEventPublisher deviceEventPublisher(
        final DeviceEventConnection deviceEventConnection,
        final DeviceEventsProperties properties
    ) {
        return new DeviceEventPublisher(
            deviceEventConnection.rabbitOperations(), properties.exchange(), properties.confirmTimeout());
    }
}

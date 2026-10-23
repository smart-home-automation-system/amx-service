package cloud.cholewa.amx.config;

import cloud.cholewa.commons.notification.NotificationConnection;
import cloud.cholewa.commons.notification.NotificationPublisher;
import org.springframework.amqp.rabbit.connection.ConnectionNameStrategy;
import org.springframework.boot.amqp.autoconfigure.RabbitProperties;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

//the notification exchange lives on another virtual host, and a connection belongs to exactly one,
//so publishing there takes a connection of its own. NotificationConnection of cholewa-commons builds
//its connection factory and its template by hand and keeps both to itself, and so must this class:
//RabbitAutoConfiguration backs off from any ConnectionFactory or RabbitOperations bean, and the
//temperature publisher would lose the auto-configured template together with everything
//spring.rabbitmq.* sets on it (observation, retry). For the same reason the auto-configured
//publisher of the library (cholewa.notification.enabled) stays switched off here: it publishes over
//the connection of the application, which leads to /temperature.
//The same class lives in heating-service, blinds-service, weather-service and light-service - a
//fifth copy kept here by the owner's choice (HAS-309); a change goes into every copy
@Configuration
public class NotificationRabbitConfig {

    //the same broker as spring.rabbitmq.*, with its connection timeout and heartbeat. Nothing is
    //opened here - the connection is made by the first publish. The strategy is the one that names
    //the connection of the application; this one gets the same name with "/notification" added.
    //Closed with the context: Spring finds close() of an AutoCloseable bean by itself
    @Bean
    NotificationConnection notificationConnection(
        final RabbitProperties rabbitProperties,
        final NotificationConnectionProperties properties,
        final ConnectionNameStrategy connectionNameStrategy,
        final ApplicationContext applicationContext
    ) {
        return NotificationConnection.create(
            new NotificationConnection.Settings(
                rabbitProperties.determineHost(),
                rabbitProperties.determinePort(),
                properties.virtualHost(),
                properties.username(),
                properties.password(),
                rabbitProperties.getConnectionTimeout(),
                rabbitProperties.getRequestedHeartbeat()
            ),
            connectionNameStrategy,
            applicationContext
        );
    }

    @Bean
    NotificationPublisher notificationPublisher(
        final NotificationConnection notificationConnection,
        final NotificationConnectionProperties properties
    ) {
        return notificationConnection.publisher(properties.exchange(), properties.env());
    }
}

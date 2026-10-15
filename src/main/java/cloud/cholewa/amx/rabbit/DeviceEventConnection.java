package cloud.cholewa.amx.rabbit;

import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.connection.ConnectionNameStrategy;
import org.springframework.amqp.rabbit.core.RabbitOperations;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.support.converter.MessageConverter;
import org.springframework.boot.amqp.autoconfigure.RabbitProperties;
import org.springframework.context.ApplicationContext;
import org.springframework.context.event.ContextClosedEvent;

import java.time.Duration;

/**
 * The connection to the virtual host of the devices ({@code /device}). A connection belongs to
 * exactly one virtual host, and the one of {@code spring.rabbitmq.*} leads to {@code /temperature},
 * so the events take a second one.
 * <p>
 * The connection factory and the template are built by hand and kept inside this object <b>on
 * purpose</b>: {@code RabbitAutoConfiguration} backs off from any {@code ConnectionFactory} or
 * {@code RabbitOperations} bean, and the temperature publisher would lose the auto-configured
 * template together with everything {@code spring.rabbitmq.template.*} sets on it (observation,
 * retry). The shape is {@code NotificationConnection} of {@code cholewa-commons}, which names its
 * connection {@code /notification} and logs its returns as notifications - hence a class of this
 * service and not that one.
 * <p>
 * Nothing is opened here: the connection is made by the first publish. {@link #close()} is final.
 */
@Slf4j
public final class DeviceEventConnection implements AutoCloseable {

    /** What the name of this connection adds to the name of the first one of the same pod. */
    public static final String CONNECTION_NAME_SUFFIX = "/device";

    private final CachingConnectionFactory connectionFactory;
    private final RabbitTemplate rabbitTemplate;
    private final ApplicationContext applicationContext;

    private DeviceEventConnection(
        final CachingConnectionFactory connectionFactory,
        final RabbitTemplate rabbitTemplate,
        final ApplicationContext applicationContext
    ) {
        this.connectionFactory = connectionFactory;
        this.rabbitTemplate = rabbitTemplate;
        this.applicationContext = applicationContext;
    }

    /**
     * @param broker                 host, port, connection timeout and heartbeat - those of
     *                               {@code spring.rabbitmq.*}
     * @param connectionNameStrategy the strategy naming the first connection of the application; this
     *                               one gets the same name with {@link #CONNECTION_NAME_SUFFIX}
     * @param messageConverter       the converter of the temperature messages, so both write JSON alike
     * @param applicationContext     where the template finds the {@code ObservationRegistry}, so that
     *                               a published event carries the trace of its frame
     */
    public static DeviceEventConnection create(
        final RabbitProperties broker,
        final String virtualHost,
        final String username,
        final String password,
        final ConnectionNameStrategy connectionNameStrategy,
        final MessageConverter messageConverter,
        final ApplicationContext applicationContext
    ) {
        final CachingConnectionFactory connectionFactory =
            new CachingConnectionFactory(broker.determineHost(), broker.determinePort());
        //the context the factory takes the closing signal for - see close()
        connectionFactory.setApplicationContext(applicationContext);
        connectionFactory.setVirtualHost(virtualHost);
        connectionFactory.setUsername(username);
        connectionFactory.setPassword(password);
        connectionFactory.setConnectionNameStrategy(factory -> {
            final String name = connectionNameStrategy.obtainNewConnectionName(factory);
            return name == null ? null : name + CONNECTION_NAME_SUFFIX;
        });
        //a message matching no binding is dropped by the broker without a word; with returns it
        //comes back, and with correlated confirms the publisher learns the outcome of each send
        connectionFactory.setPublisherReturns(true);
        connectionFactory.setPublisherConfirmType(CachingConnectionFactory.ConfirmType.CORRELATED);
        final Duration connectionTimeout = broker.getConnectionTimeout();
        if (connectionTimeout != null) {
            connectionFactory.setConnectionTimeout(Math.toIntExact(connectionTimeout.toMillis()));
        }
        final Duration heartbeat = broker.getRequestedHeartbeat();
        if (heartbeat != null) {
            connectionFactory.setRequestedHeartBeat(Math.toIntExact(heartbeat.toSeconds()));
        }

        final RabbitTemplate rabbitTemplate = new RabbitTemplate(connectionFactory);
        rabbitTemplate.setMessageConverter(messageConverter);
        rabbitTemplate.setMandatory(true);
        //the publisher fails the send on a returned message; this is the line that says why
        rabbitTemplate.setReturnsCallback(returned -> log.error(
            "Device event was not routed to any queue: {} - {} (routing key {})",
            returned.getReplyCode(), returned.getReplyText(), returned.getRoutingKey()));
        //spring.rabbitmq.template.observation-enabled reaches only the auto-configured template;
        //this one finds the ObservationRegistry through the application context, so it needs both
        rabbitTemplate.setObservationEnabled(true);
        rabbitTemplate.setApplicationContext(applicationContext);
        rabbitTemplate.afterPropertiesSet();

        return new DeviceEventConnection(connectionFactory, rabbitTemplate, applicationContext);
    }

    /** The template of this connection. Must not be exposed as a bean - see the class comment. */
    public RabbitOperations rabbitOperations() {
        return rabbitTemplate;
    }

    //destroy() alone only resets the connection - the next publish would open a new one, which
    //nothing closes any more. A CachingConnectionFactory stops for good only when it was told that
    //its application context closed; built by hand it hears nothing, so it is told here
    @Override
    public void close() {
        connectionFactory.onApplicationEvent(new ContextClosedEvent(applicationContext));
        connectionFactory.destroy();
    }
}

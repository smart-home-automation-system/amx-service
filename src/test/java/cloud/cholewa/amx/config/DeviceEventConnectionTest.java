package cloud.cholewa.amx.config;

import cloud.cholewa.amx.rabbit.DeviceEventConnection;
import cloud.cholewa.amx.rabbit.DeviceEventPublisher;
import cloud.cholewa.home.model.DeviceEvent;
import cloud.cholewa.home.model.EatonGatewayType;
import cloud.cholewa.home.model.EatonRxEvent;
import cloud.cholewa.home.model.SmartVendor;
import org.junit.jupiter.api.Test;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.connection.ConnectionNameStrategy;
import org.springframework.amqp.rabbit.core.RabbitOperations;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.support.converter.JacksonJsonMessageConverter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.ApplicationContext;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.util.ReflectionTestUtils;

import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;

import static org.assertj.core.api.Assertions.assertThat;

//the second connection, to /device, in the whole application: its factory and its template stay
//inside DeviceEventConnection, so the auto-configured ones - and with them the temperature
//publisher's observation and retry - are still there. Nothing here opens a connection
@SpringBootTest(properties = "HOSTNAME=amx-service-5968757896-bghz5")
@ActiveProfiles("test")
class DeviceEventConnectionTest {

    @Autowired
    private ApplicationContext context;

    @Autowired
    private DeviceEventConnection deviceEventConnection;

    @Autowired
    private DeviceEventPublisher deviceEventPublisher;

    @Test
    void should_leave_the_auto_configured_factory_and_template_the_only_beans() {
        assertThat(context.getBeansOfType(ConnectionFactory.class)).hasSize(1);
        assertThat(context.getBeansOfType(RabbitOperations.class)).hasSize(1);
        assertThat(context.getBean(RabbitOperations.class)).isNotSameAs(deviceEventConnection.rabbitOperations());
    }

    @Test
    void should_connect_to_the_virtual_host_of_the_devices_as_its_user() {
        final CachingConnectionFactory factory = factory();

        assertThat(factory.getVirtualHost()).isEqualTo("/device");
        assertThat(factory.getUsername()).isEqualTo("device");
    }

    //the management UI tells the two connections of one pod apart by the suffix
    @Test
    void should_name_the_connection_after_the_pod_and_its_purpose() {
        final ConnectionNameStrategy strategy =
            (ConnectionNameStrategy) ReflectionTestUtils.getField(factory(), "connectionNameStrategy");
        assertThat(strategy.obtainNewConnectionName(factory()))
            .isEqualTo("amx-service-5968757896-bghz5/device");
    }

    //a send that returns proves nothing: confirms and returns, or an event lost on the way is
    //reported as published
    @Test
    void should_publish_with_correlated_confirms_and_mandatory_returns() {
        final RabbitTemplate template = (RabbitTemplate) deviceEventConnection.rabbitOperations();

        assertThat(factory().isPublisherConfirms()).isTrue();
        assertThat(factory().isPublisherReturns()).isTrue();
        assertThat(template.isMandatoryFor(new Message(new byte[0], new MessageProperties()))).isTrue();
        assertThat(template.getMessageConverter()).isInstanceOf(JacksonJsonMessageConverter.class);
    }

    @Test
    void should_publish_to_the_topic_exchange_of_the_devices() {
        assertThat(ReflectionTestUtils.getField(deviceEventPublisher, "exchange")).isEqualTo("amq.topic");
    }

    //what the services of the devices read: the SDK values, a local time without an offset, and
    //no value where the event carries none
    @Test
    void should_write_the_event_as_the_json_of_the_sdk() {
        final RabbitTemplate template = (RabbitTemplate) deviceEventConnection.rabbitOperations();
        final Message message = template.getMessageConverter().toMessage(DeviceEvent.builder()
            .at(LocalDateTime.of(2026, 10, 15, 18, 30, 5))
            .vendor(SmartVendor.EATON)
            .gateway(EatonGatewayType.BLINDS)
            .point(14)
            .event(EatonRxEvent.UP_PRESSED)
            .build(), new MessageProperties());

        assertThat(new String(message.getBody(), StandardCharsets.UTF_8)).isEqualTo(
            "{\"at\":\"2026-10-15T18:30:05\",\"vendor\":\"eaton\",\"gateway\":\"blinds\",\"point\":14,\"event\":\"up pressed\"}");
        assertThat(message.getMessageProperties().getContentType()).isEqualTo(MessageProperties.CONTENT_TYPE_JSON);
    }

    private CachingConnectionFactory factory() {
        return (CachingConnectionFactory) ((RabbitTemplate) deviceEventConnection.rabbitOperations()).getConnectionFactory();
    }
}

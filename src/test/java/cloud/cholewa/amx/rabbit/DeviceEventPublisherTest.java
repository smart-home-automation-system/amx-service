package cloud.cholewa.amx.rabbit;

import cloud.cholewa.home.model.DeviceEvent;
import cloud.cholewa.home.model.EatonGatewayType;
import cloud.cholewa.home.model.EatonRxEvent;
import cloud.cholewa.home.model.SmartVendor;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.amqp.AmqpConnectException;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.core.ReturnedMessage;
import org.springframework.amqp.rabbit.connection.CorrelationData;
import org.springframework.amqp.rabbit.core.RabbitOperations;
import reactor.test.StepVerifier;

import java.net.ConnectException;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;

//a publish is done when the broker took the event into a queue, and only then
@ExtendWith(MockitoExtension.class)
class DeviceEventPublisherTest {

    private static final String EXCHANGE = "amq.topic";
    private static final DeviceEvent EVENT = DeviceEvent.builder()
        .at(LocalDateTime.of(2026, 10, 15, 18, 30, 5))
        .vendor(SmartVendor.EATON)
        .gateway(EatonGatewayType.BLINDS)
        .point(14)
        .event(EatonRxEvent.STATUS)
        .value(1)
        .build();

    @Mock
    private RabbitOperations rabbitOperations;

    private final AtomicReference<String> confirmedOn = new AtomicReference<>();

    @Test
    void should_route_the_event_by_vendor_gateway_and_point() {
        assertThat(DeviceEventPublisher.routingKeyOf(EVENT)).isEqualTo("eaton.blinds.14");
        assertThat(DeviceEventPublisher.routingKeyOf(DeviceEvent.builder()
            .gateway(EatonGatewayType.LIGHTS).point(5).build())).isEqualTo("eaton.lights.5");
    }

    @Test
    void should_complete_when_the_broker_confirmed_the_event() {
        brokerAnswers(new CorrelationData.Confirm(true, null), null);

        publisher(Duration.ofSeconds(5)).publish(EVENT)
            .doOnSuccess(unused -> confirmedOn.set(Thread.currentThread().getName()))
            .as(StepVerifier::create)
            .verifyComplete();

        //never on the thread of the connection, which completes the confirmation
        assertThat(confirmedOn.get()).startsWith("boundedElastic");
    }

    @Test
    void should_fail_when_the_broker_refused_the_event() {
        brokerAnswers(new CorrelationData.Confirm(false, "no access"), null);

        publisher(Duration.ofSeconds(5)).publish(EVENT)
            .as(StepVerifier::create)
            .expectErrorSatisfies(failure -> assertThat(failure)
                .isInstanceOf(DeviceEventPublishException.class)
                .hasMessage("Device event eaton.blinds.14 refused by the broker: no access"))
            .verify();
    }

    //confirmed all the same: the broker hands back what matches no binding first
    @Test
    void should_fail_when_the_event_was_routed_to_no_queue() {
        brokerAnswers(
            new CorrelationData.Confirm(true, null),
            new ReturnedMessage(new Message(new byte[0], new MessageProperties()), 312, "NO_ROUTE", EXCHANGE, "eaton.blinds.14")
        );

        publisher(Duration.ofSeconds(5)).publish(EVENT)
            .as(StepVerifier::create)
            .expectErrorSatisfies(failure -> assertThat(failure)
                .isInstanceOf(DeviceEventPublishException.class)
                .hasMessage("Device event eaton.blinds.14 was not routed to any queue: NO_ROUTE"))
            .verify();
    }

    @Test
    void should_fail_when_the_broker_did_not_confirm_in_time() {
        //the send goes through, the confirmation never comes

        publisher(Duration.ofMillis(200)).publish(EVENT)
            .as(StepVerifier::create)
            .expectErrorSatisfies(failure -> assertThat(failure)
                .isInstanceOf(DeviceEventPublishException.class)
                .hasMessage("Device event eaton.blinds.14 not taken by the broker within PT0.2S"))
            .verify(Duration.ofSeconds(5));
    }

    //a send stuck on opening the connection holds the next frame no longer than a confirmation
    @Test
    void should_fail_when_the_send_itself_takes_too_long() {
        doAnswer(invocation -> {
            Thread.sleep(2_000);
            return null;
        }).when(rabbitOperations).convertAndSend(eq(EXCHANGE), eq("eaton.blinds.14"), eq(EVENT), any(CorrelationData.class));

        publisher(Duration.ofMillis(200)).publish(EVENT)
            .as(StepVerifier::create)
            .expectErrorSatisfies(failure -> assertThat(failure)
                .isInstanceOf(DeviceEventPublishException.class)
                .hasMessage("Device event eaton.blinds.14 not taken by the broker within PT0.2S"))
            .verify(Duration.ofSeconds(1));
    }

    @Test
    void should_fail_when_the_event_could_not_be_sent() {
        doThrow(new AmqpConnectException(new ConnectException("Connection refused")))
            .when(rabbitOperations).convertAndSend(eq(EXCHANGE), eq("eaton.blinds.14"), eq(EVENT), any(CorrelationData.class));

        publisher(Duration.ofSeconds(5)).publish(EVENT)
            .as(StepVerifier::create)
            .expectErrorSatisfies(failure -> assertThat(failure)
                .isInstanceOf(DeviceEventPublishException.class)
                .hasMessageStartingWith("Device event eaton.blinds.14 not sent: ")
                .hasCauseInstanceOf(AmqpConnectException.class))
            .verify();
    }

    private DeviceEventPublisher publisher(final Duration confirmTimeout) {
        return new DeviceEventPublisher(rabbitOperations, EXCHANGE, confirmTimeout);
    }

    //what the broker does on the thread of the connection: hand back an unroutable message, then
    //confirm
    private void brokerAnswers(final CorrelationData.Confirm confirm, final ReturnedMessage returned) {
        doAnswer(invocation -> {
            final CorrelationData correlationData = invocation.getArgument(3);
            final Thread connection = new Thread(() -> {
                correlationData.setReturned(returned);
                correlationData.getFuture().complete(confirm);
            }, "AMQP Connection");
            connection.start();
            return null;
        }).when(rabbitOperations).convertAndSend(eq(EXCHANGE), eq("eaton.blinds.14"), eq(EVENT), any(CorrelationData.class));
    }
}

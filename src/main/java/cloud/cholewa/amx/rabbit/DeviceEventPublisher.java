package cloud.cholewa.amx.rabbit;

import cloud.cholewa.home.model.DeviceEvent;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.rabbit.connection.CorrelationData;
import org.springframework.amqp.rabbit.core.RabbitOperations;
import org.springframework.util.Assert;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.time.Duration;
import java.util.concurrent.TimeoutException;

/**
 * Publishes what an Eaton device reported to the topic exchange of {@code /device}, with the
 * routing key {@code eaton.<gateway>.<point>}. Every queue of the device services is bound with
 * {@code #}, so each of them gets every event and filters by the registry.
 * <p>
 * A publish completes only when the broker has taken the event into a queue - confirmed and not
 * handed back. Every other outcome is a {@link DeviceEventPublishException}; nothing is retried
 * here: the event is the state of the device at that moment, and the next frame of the device
 * says it again.
 */
@Slf4j
public class DeviceEventPublisher {

    static final String ROUTING_KEY_PREFIX = "eaton.";

    private final RabbitOperations rabbitOperations;
    private final String exchange;
    private final Duration confirmTimeout;

    public DeviceEventPublisher(
        final RabbitOperations rabbitOperations,
        final String exchange,
        final Duration confirmTimeout
    ) {
        Assert.notNull(rabbitOperations, "RabbitOperations must not be null");
        Assert.hasText(exchange, "The exchange of the device events must not be blank");
        Assert.isTrue(
            confirmTimeout != null && confirmTimeout.isPositive(), "The confirm timeout must be positive");
        this.rabbitOperations = rabbitOperations;
        this.exchange = exchange;
        this.confirmTimeout = confirmTimeout;
    }

    public Mono<Void> publish(final DeviceEvent event) {
        final String routingKey = routingKeyOf(event);
        return Mono.fromCallable(() -> {
                final CorrelationData correlationData = new CorrelationData();
                rabbitOperations.convertAndSend(exchange, routingKey, event, correlationData);
                return correlationData;
            })
            //the send blocks on the broker connection - it may be opening it
            .subscribeOn(Schedulers.boundedElastic())
            .onErrorMap(failure -> new DeviceEventPublishException(
                    "Device event " + routingKey + " not sent: " + failure.getMessage(), failure))
            .flatMap(correlationData -> Mono.fromFuture(correlationData.getFuture())
                .timeout(confirmTimeout)
                .onErrorMap(TimeoutException.class, timeout -> new DeviceEventPublishException(
                    "Device event " + routingKey + " not confirmed by the broker within " + confirmTimeout, timeout))
                //Spring AMQP completes the future on the thread that reads the frames of the
                //connection, inside the lock of the channel: whatever follows - the next frame of
                //the receiver - must not run there (the lesson of NotificationPublisher)
                .publishOn(Schedulers.boundedElastic())
                .flatMap(confirm -> verifyDelivery(routingKey, correlationData, confirm)))
            .doOnSuccess(unused -> log.info("Published device event {}: {}", routingKey, event.getEvent()));
    }

    static String routingKeyOf(final DeviceEvent event) {
        return ROUTING_KEY_PREFIX + event.getGateway().getValue() + "." + event.getPoint();
    }

    private static Mono<Void> verifyDelivery(
        final String routingKey,
        final CorrelationData correlationData,
        final CorrelationData.Confirm confirm
    ) {
        if (!confirm.ack()) {
            return Mono.error(new DeviceEventPublishException(
                "Device event " + routingKey + " refused by the broker: " + confirm.reason()));
        }
        //a message matching no binding is confirmed all the same - the broker hands it back first
        if (correlationData.getReturned() != null) {
            return Mono.error(new DeviceEventPublishException(
                "Device event " + routingKey + " was not routed to any queue: "
                    + correlationData.getReturned().getReplyText()));
        }
        return Mono.empty();
    }
}

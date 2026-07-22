package cloud.cholewa.amx.rabbit;

import cloud.cholewa.home.model.RoomName;
import cloud.cholewa.home.model.TemperatureMessage;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.time.LocalDateTime;

@Slf4j
@Service
@RequiredArgsConstructor
public class TemperaturePublisher {

    private final RabbitTemplate rabbitTemplate;

    public Mono<Void> publish(final double temperature, final RoomName room) {
        return Mono.fromRunnable(() -> {
                log.debug("Publishing temperature: {} for room: {}", temperature, room);
                rabbitTemplate.convertAndSend(
                    "temperature.events",
                    "",
                    TemperatureMessage.builder()
                        .date(LocalDateTime.now())
                        .room(room)
                        .temperature(temperature)
                        .build()
                );
            })
            .subscribeOn(Schedulers.boundedElastic())
            .then();
    }
}

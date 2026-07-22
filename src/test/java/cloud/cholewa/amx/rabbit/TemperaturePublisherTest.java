package cloud.cholewa.amx.rabbit;

import cloud.cholewa.home.model.RoomName;
import cloud.cholewa.home.model.TemperatureMessage;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import reactor.test.StepVerifier;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

@ExtendWith(MockitoExtension.class)
class TemperaturePublisherTest {

    @Mock
    private RabbitTemplate rabbitTemplate;

    @InjectMocks
    private TemperaturePublisher sut;

    @Test
    void should_publish_temperature_message() {
        double temperature = 22.5;
        RoomName room = RoomName.LIVING_ROOM;

        sut.publish(temperature, room)
            .as(StepVerifier::create)
            .verifyComplete();

        verify(rabbitTemplate, times(1))
            .convertAndSend(eq("temperature.events"), eq(""), any(TemperatureMessage.class));

        verifyNoMoreInteractions(rabbitTemplate);
    }
}

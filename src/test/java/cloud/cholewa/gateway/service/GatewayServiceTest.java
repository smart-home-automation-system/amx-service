package cloud.cholewa.gateway.service;

import cloud.cholewa.gateway.device.client.DeviceDatabaseClient;
import cloud.cholewa.gateway.infrastructure.error.ConfigurationCallException;
import cloud.cholewa.gateway.infrastructure.error.EatonException;
import cloud.cholewa.gateway.infrastructure.error.EatonParsingException;
import cloud.cholewa.gateway.rabbit.TemperaturePublisher;
import cloud.cholewa.home.model.EatonConfigurationResponse;
import cloud.cholewa.home.model.EatonDatagramReply;
import cloud.cholewa.home.model.EatonGatewayType;
import cloud.cholewa.home.model.RoomName;
import cloud.cholewa.home.model.SmartDeviceType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Answers;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.http.HttpStatus;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.util.Collections;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyDouble;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class GatewayServiceTest {

    @Mock(answer = Answers.RETURNS_SMART_NULLS)
    private DeviceDatabaseClient deviceDatabaseClient;

    @Mock(answer = Answers.RETURNS_SMART_NULLS)
    private TemperaturePublisher temperaturePublisher;

    @InjectMocks
    private GatewayService sut;

    @Test
    void should_throw_exception_when_received_invalid_eaton_message() {
        final EatonDatagramReply invalidMessage = EatonDatagramReply.builder()
            .message("invalid message")
            .build();

        sut.consumeAmxMessage(invalidMessage)
            .as(StepVerifier::create)
            .expectErrorSatisfies(throwable -> {
                assertThat(throwable)
                    .isInstanceOf(EatonException.class)
                    .hasMessageContaining("Invalid Eaton message");
            })
            .verify();

        verify(deviceDatabaseClient, never()).getEatonConfiguration(anyInt(), any());
        verify(temperaturePublisher, never()).publish(anyDouble(), any());
    }

    @Test
    void should_throw_exception_when_point_extraction_fails() {
        final EatonDatagramReply invalidMessage = EatonDatagramReply.builder()
            .message("5A,C,C1,AA,70,32,10,00,00,00,00,00,00,A5")
            .build();

        sut.consumeAmxMessage(invalidMessage)
            .as(StepVerifier::create)
            .expectErrorSatisfies(throwable -> {
                assertThat(throwable)
                    .isInstanceOf(EatonParsingException.class)
                    .hasMessageContaining("Invalid data point number");
            })
            .verify();

        verify(deviceDatabaseClient, never()).getEatonConfiguration(anyInt(), any());
        verify(temperaturePublisher, never()).publish(anyDouble(), any());
    }

    @Test
    void should_throw_exception_when_no_configuration_found() {
        final EatonDatagramReply invalidMessage = EatonDatagramReply.builder()
            .gateway(EatonGatewayType.BLINDS)
            .message("5A,C,C1,12,70,32,10,00,00,00,00,00,00,A5")
            .build();

        when(deviceDatabaseClient.getEatonConfiguration(anyInt(), any()))
            .thenReturn(Mono.error(new ConfigurationCallException(HttpStatus.NOT_FOUND, Collections.emptySet())));

        sut.consumeAmxMessage(invalidMessage)
            .as(StepVerifier::create)
            .expectErrorSatisfies(throwable -> {
                assertThat(throwable)
                    .isInstanceOf(ConfigurationCallException.class);
            })
            .verify();

        verify(deviceDatabaseClient, times(1)).getEatonConfiguration(anyInt(), any());
        verify(temperaturePublisher, never()).publish(anyDouble(), any());

        verifyNoMoreInteractions(deviceDatabaseClient);
    }

    @Test
    void should_throw_exception_when_type_is_not_temperature_sensor() {
        final EatonDatagramReply invalidMessage = EatonDatagramReply.builder()
            .gateway(EatonGatewayType.BLINDS)
            .message("5A,C,C1,12,70,32,10,00,00,00,00,00,00,A5")
            .build();

        when(deviceDatabaseClient.getEatonConfiguration(anyInt(), any()))
            .thenReturn(Mono.just(EatonConfigurationResponse.builder()
                .point(1)
                .type(SmartDeviceType.TEMPERATURE_SENSOR)
                .room(RoomName.KITCHEN)
                .build()));

        sut.consumeAmxMessage(invalidMessage)
            .as(StepVerifier::create)
            .expectErrorSatisfies(throwable -> {
                assertThat(throwable)
                    .isInstanceOf(EatonParsingException.class)
                    .hasMessageContaining("Invalid device type for temperature sensor");
            })
            .verify();

        verify(deviceDatabaseClient, times(1)).getEatonConfiguration(anyInt(), any());
        verify(temperaturePublisher, never()).publish(anyDouble(), any());
        verifyNoMoreInteractions(deviceDatabaseClient);
    }

    @Test
    void should_publish_temperature_when_temperature_sensor_message_received() {
        when(deviceDatabaseClient.getEatonConfiguration(anyInt(), any()))
            .thenReturn(Mono.just(EatonConfigurationResponse.builder()
                .point(1)
                .type(SmartDeviceType.TEMPERATURE_SENSOR)
                .room(RoomName.KITCHEN)
                .build()));

        when(temperaturePublisher.publish(anyDouble(), any())).thenReturn(Mono.empty());

        final EatonDatagramReply invalidMessage = EatonDatagramReply.builder()
            .gateway(EatonGatewayType.BLINDS)
            .message("5A,C,C1,12,62,17,10,00,00,00,00,00,00,A5")
            .build();

        sut.consumeAmxMessage(invalidMessage)
            .as(StepVerifier::create)
            .verifyComplete();

        verify(deviceDatabaseClient, times(1)).getEatonConfiguration(anyInt(), any());
        verify(temperaturePublisher, times(1)).publish(anyDouble(), any());

        verifyNoMoreInteractions(deviceDatabaseClient, temperaturePublisher);
    }
}


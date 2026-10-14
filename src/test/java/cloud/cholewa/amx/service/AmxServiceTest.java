package cloud.cholewa.amx.service;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import cloud.cholewa.amx.device.client.DeviceDatabaseClient;
import cloud.cholewa.amx.frameserver.ReceivedFrame;
import cloud.cholewa.amx.infrastructure.error.ConfigurationCallException;
import cloud.cholewa.amx.infrastructure.error.EatonParsingException;
import cloud.cholewa.amx.rabbit.TemperaturePublisher;
import cloud.cholewa.home.model.EatonConfigurationResponse;
import cloud.cholewa.home.model.EatonGatewayType;
import cloud.cholewa.home.model.RoomName;
import cloud.cholewa.home.model.SmartDeviceType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Answers;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.time.LocalDateTime;
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyDouble;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class AmxServiceTest {

    private static final LocalDateTime RECEIVED_AT = LocalDateTime.of(2026, 10, 14, 20, 15, 30);

    private final ListAppender<ILoggingEvent> serviceLog = new ListAppender<>();

    @Mock(answer = Answers.RETURNS_SMART_NULLS)
    private DeviceDatabaseClient deviceDatabaseClient;

    @Mock(answer = Answers.RETURNS_SMART_NULLS)
    private TemperaturePublisher temperaturePublisher;

    @InjectMocks
    private AmxService sut;

    @BeforeEach
    void captureTheLog() {
        serviceLog.start();
        serviceLogger().addAppender(serviceLog);
    }

    @AfterEach
    void releaseTheLog() {
        serviceLogger().detachAppender(serviceLog);
    }

    @Test
    void should_throw_exception_when_received_invalid_eaton_message() {
        sut.consume(blinds("invalid message"))
            .as(StepVerifier::create)
            .expectErrorSatisfies(throwable -> {
                assertThat(throwable)
                    .isInstanceOf(EatonParsingException.class)
                    .hasMessageContaining("Invalid Eaton message");
            })
            .verify();

        verify(deviceDatabaseClient, never()).getEatonConfiguration(anyInt(), any());
        verify(temperaturePublisher, never()).publish(anyDouble(), any(), any());
    }

    @Test
    void should_throw_exception_when_point_extraction_fails() {
        sut.consume(blinds("5A,C,C1,AA,70,32,10,00,00,00,00,00,00,A5"))
            .as(StepVerifier::create)
            .expectErrorSatisfies(throwable -> {
                assertThat(throwable)
                    .isInstanceOf(EatonParsingException.class)
                    .hasMessageContaining("Invalid data point number");
            })
            .verify();

        verify(deviceDatabaseClient, never()).getEatonConfiguration(anyInt(), any());
        verify(temperaturePublisher, never()).publish(anyDouble(), any(), any());
    }

    @Test
    void should_throw_exception_when_no_configuration_found() {
        when(deviceDatabaseClient.getEatonConfiguration(anyInt(), any()))
            .thenReturn(Mono.error(new ConfigurationCallException(HttpStatus.NOT_FOUND, Collections.emptySet())));

        sut.consume(blinds("5A,C,C1,12,70,32,10,00,00,00,00,00,00,A5"))
            .as(StepVerifier::create)
            .expectErrorSatisfies(throwable -> {
                assertThat(throwable)
                    .isInstanceOf(ConfigurationCallException.class);
            })
            .verify();

        verify(deviceDatabaseClient).getEatonConfiguration(anyInt(), any());
        verify(temperaturePublisher, never()).publish(anyDouble(), any(), any());

        verifyNoMoreInteractions(deviceDatabaseClient);
    }

    @Test
    void should_throw_exception_when_the_frame_of_a_temperature_sensor_carries_no_temperature() {
        when(deviceDatabaseClient.getEatonConfiguration(anyInt(), any()))
            .thenReturn(Mono.just(EatonConfigurationResponse.builder()
                .point(1)
                .type(SmartDeviceType.TEMPERATURE_SENSOR)
                .room(RoomName.KITCHEN)
                .build()));

        sut.consume(blinds("5A,C,C1,12,70,32,10,00,00,00,00,00,00,A5"))
            .as(StepVerifier::create)
            .expectErrorSatisfies(throwable -> {
                assertThat(throwable)
                    .isInstanceOf(EatonParsingException.class)
                    .hasMessageContaining("Invalid device type for temperature sensor");
            })
            .verify();

        verify(deviceDatabaseClient).getEatonConfiguration(anyInt(), any());
        verify(temperaturePublisher, never()).publish(anyDouble(), any(), any());
        verifyNoMoreInteractions(deviceDatabaseClient);
    }

    //the actuators report too - their status after a command, a button pressed - and until HAS-229
    //nobody takes those: a line in the log, never a failure that would look like a broken frame
    @ParameterizedTest
    @EnumSource(value = SmartDeviceType.class, names = {"BLINDS", "LIGHT", "DIMMER", "OTHER"})
    void should_complete_without_publishing_when_the_device_is_not_a_temperature_sensor(final SmartDeviceType type) {
        when(deviceDatabaseClient.getEatonConfiguration(anyInt(), any()))
            .thenReturn(Mono.just(EatonConfigurationResponse.builder()
                .point(14)
                .type(type)
                .room(RoomName.KITCHEN)
                .build()));

        sut.consume(blinds("5A,0C,C1,0E,70,01,00,00,00,00,00,3A,10,A5"))
            .as(StepVerifier::create)
            .verifyComplete();

        verify(deviceDatabaseClient).getEatonConfiguration(14, EatonGatewayType.BLINDS);
        verifyNoInteractions(temperaturePublisher);
        assertThat(serviceLog.list).extracting(ILoggingEvent::getLevel).containsExactly(Level.WARN);
    }

    //the gateway's acknowledgement of a command - this service's or the other master's - carries
    //no data point: there is nothing to look up, and 1C would have been asked for as point 28.
    //STT_OK with STS_OK_MRF is the telegram sent, whatever the data bytes after them
    @ParameterizedTest
    @ValueSource(strings = {
        "5A,08,C3,1C,04,00,00,00,00,A5",
        "5A,08,C3,1C,04,00,10,00,00,A5"
    })
    void should_take_an_acknowledgement_without_a_lookup(final String frame) {
        sut.consume(new ReceivedFrame(EatonGatewayType.LIGHTS, frame, RECEIVED_AT))
            .as(StepVerifier::create)
            .verifyComplete();

        verifyNoInteractions(deviceDatabaseClient, temperaturePublisher);
        assertThat(serviceLog.list).singleElement().satisfies(event -> {
            assertThat(event.getLevel()).isEqualTo(Level.INFO);
            assertThat(event.getFormattedMessage()).isEqualTo("Gateway lights took a telegram: [" + frame + "]");
        });
    }

    //STT_ERROR: the telegram was not sent, and the caller of the command already got its 202 -
    //the line in the log is all that tells it
    @ParameterizedTest
    @CsvSource(delimiter = '|', value = {
        "5A,08,C3,09,01,00,00,00,00,A5 | unknown command",
        "5A,08,C3,09,02,00,00,00,00,A5 | data point out of range",
        "5A,08,C3,09,03,00,00,00,00,A5 | RF busy, the telegram was lost",
        "5A,08,C3,09,04,00,00,00,00,A5 | RF busy receiving",
        "5A,08,C3,09,07,00,00,00,00,A5 | error 07"
    })
    void should_warn_about_a_telegram_the_gateway_did_not_send(final String frame, final String reason) {
        sut.consume(new ReceivedFrame(EatonGatewayType.LIGHTS, frame, RECEIVED_AT))
            .as(StepVerifier::create)
            .verifyComplete();

        verifyNoInteractions(deviceDatabaseClient, temperaturePublisher);
        assertThat(serviceLog.list).singleElement().satisfies(event -> {
            assertThat(event.getLevel()).isEqualTo(Level.WARN);
            assertThat(event.getFormattedMessage())
                .isEqualTo("Gateway lights did not send a telegram: " + reason + " [" + frame + "]");
        });
    }

    //any other status - the time account, say - is told and nothing more
    @Test
    void should_take_another_status_without_a_lookup() {
        sut.consume(blinds("5A,08,C3,0A,0F,00,00,00,00,A5"))
            .as(StepVerifier::create)
            .verifyComplete();

        verifyNoInteractions(deviceDatabaseClient, temperaturePublisher);
        assertThat(serviceLog.list).singleElement().satisfies(event -> {
            assertThat(event.getLevel()).isEqualTo(Level.INFO);
            assertThat(event.getFormattedMessage())
                .isEqualTo("Gateway blinds sent a status of type 0A: [5A,08,C3,0A,0F,00,00,00,00,A5]");
        });
    }

    //a status frame that is not one: the validator's word, not a lookup of whatever is in it
    @Test
    void should_throw_exception_when_a_status_frame_is_cut_off() {
        sut.consume(blinds("5A,08,C3,1C,A5"))
            .as(StepVerifier::create)
            .expectError(EatonParsingException.class)
            .verify();

        verifyNoInteractions(deviceDatabaseClient, temperaturePublisher);
    }

    //a text of the gateway, 32 bytes, forwarded like any frame: a line in the log, not a failure
    @Test
    void should_take_an_error_frame_without_a_lookup() {
        sut.consume(blinds("5A,1E,C9,45,52,52,4F,52,3A,20,52,46,20,62,75,73,79,00,00,00,00,00,00,00,00,00,00,00,00,00,00,00,A5"))
            .as(StepVerifier::create)
            .verifyComplete();

        verifyNoInteractions(deviceDatabaseClient, temperaturePublisher);
        assertThat(serviceLog.list).extracting(ILoggingEvent::getLevel).containsExactly(Level.WARN);
    }

    @Test
    void should_normalize_lowercase_frame_and_publish_negative_temperature() {
        when(deviceDatabaseClient.getEatonConfiguration(anyInt(), any()))
            .thenReturn(Mono.just(EatonConfigurationResponse.builder()
                .point(18)
                .type(SmartDeviceType.TEMPERATURE_SENSOR)
                .room(RoomName.KITCHEN)
                .build()));

        when(temperaturePublisher.publish(anyDouble(), any(), any())).thenReturn(Mono.empty());

        sut.consume(blinds("5a,c,c1,12,62,3,0,ff,db,0,0,44,5,a5"))
            .as(StepVerifier::create)
            .verifyComplete();

        verify(deviceDatabaseClient).getEatonConfiguration(anyInt(), any());
        verify(temperaturePublisher).publish(-3.7, RoomName.KITCHEN, RECEIVED_AT);

        verifyNoMoreInteractions(deviceDatabaseClient, temperaturePublisher);
    }

    //what the frame server really sends: every byte as two hex digits. The reading carries the
    //time the frame arrived
    @Test
    void should_publish_temperature_when_temperature_sensor_message_received() {
        when(deviceDatabaseClient.getEatonConfiguration(anyInt(), any()))
            .thenReturn(Mono.just(EatonConfigurationResponse.builder()
                .point(45)
                .type(SmartDeviceType.TEMPERATURE_SENSOR)
                .room(RoomName.KITCHEN)
                .build()));

        when(temperaturePublisher.publish(anyDouble(), any(), any())).thenReturn(Mono.empty());

        sut.consume(blinds("5A,0C,C1,2D,62,17,00,00,F0,00,00,3A,10,A5"))
            .as(StepVerifier::create)
            .verifyComplete();

        verify(deviceDatabaseClient).getEatonConfiguration(45, EatonGatewayType.BLINDS);
        verify(temperaturePublisher).publish(24.0, RoomName.KITCHEN, RECEIVED_AT);

        verifyNoMoreInteractions(deviceDatabaseClient, temperaturePublisher);
    }

    private static ReceivedFrame blinds(final String frame) {
        return new ReceivedFrame(EatonGatewayType.BLINDS, frame, RECEIVED_AT);
    }

    private static Logger serviceLogger() {
        return (Logger) LoggerFactory.getLogger(AmxService.class);
    }
}

package cloud.cholewa.amx.service;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import cloud.cholewa.amx.device.client.DeviceDatabaseClient;
import cloud.cholewa.amx.device.client.DeviceDatabaseClientConfig;
import cloud.cholewa.amx.frameserver.FrameServerClient;
import cloud.cholewa.amx.frameserver.ReceivedFrame;
import cloud.cholewa.amx.rabbit.DeviceEventPublisher;
import cloud.cholewa.amx.rabbit.TemperaturePublisher;
import cloud.cholewa.home.model.EatonGatewayType;
import cloud.cholewa.home.model.RoomName;
import io.micrometer.observation.ObservationRegistry;
import lombok.SneakyThrows;
import mockwebserver3.MockResponse;
import mockwebserver3.MockWebServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.client.reactive.ReactorClientHttpConnector;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;
import reactor.netty.http.client.HttpClient;
import reactor.test.StepVerifier;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyDouble;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

//The whole way of a frame through the lookup - what database-service answers on one side, the level
//of the log line on the other. EatonFrameReceiverTest stubs the service and DeviceDatabaseClientTest
//stops at the exception, so neither shows that the code in a downstream body decides the level the
//alerts depend on (HAS-176): a 404 that names the unknown data point is a WARN, a 404 without the
//code is a path nothing answers under - an outage - and an ERROR like every other failure
@ExtendWith(MockitoExtension.class)
class EatonFrameLookupTest {

    private static final LocalDateTime RECEIVED_AT = LocalDateTime.of(2026, 10, 14, 20, 15, 30);
    private static final ReceivedFrame FRAME =
        new ReceivedFrame(EatonGatewayType.BLINDS, "5A,0C,C1,12,62,17,00,00,F0,00,00,3A,10,A5", RECEIVED_AT);

    //not a few hundred milliseconds: the first call of a fresh WebClient initialises Netty, and on a
    //CI runner that alone must not turn an answer into a 504. Above the 2 s DownstreamErrors gives an
    //error body
    private static final Duration RESPONSE_TIMEOUT = Duration.ofSeconds(3);

    private final ListAppender<ILoggingEvent> receiverLog = new ListAppender<>();

    @Mock
    private FrameServerClient frameServerClient;

    @Mock
    private TemperaturePublisher temperaturePublisher;

    @Mock
    private DeviceEventPublisher deviceEventPublisher;

    private MockWebServer databaseService;
    private EatonFrameReceiver sut;

    @SneakyThrows
    @BeforeEach
    void setUp() {
        //a port of its own for every test and a client without a pool (HAS-216)
        databaseService = new MockWebServer();
        databaseService.start();
        final DeviceDatabaseClient deviceDatabaseClient = new DeviceDatabaseClient(
            WebClient.builder().clientConnector(new ReactorClientHttpConnector(HttpClient.newConnection())).build(),
            new DeviceDatabaseClientConfig("localhost", String.valueOf(databaseService.getPort()), RESPONSE_TIMEOUT)
        );
        sut = new EatonFrameReceiver(
            frameServerClient, new AmxService(deviceDatabaseClient, temperaturePublisher, deviceEventPublisher), ObservationRegistry.NOOP);

        receiverLog.start();
        receiverLogger().addAppender(receiverLog);
    }

    @SneakyThrows
    @AfterEach
    void tearDown() {
        receiverLogger().detachAppender(receiverLog);
        databaseService.close();
    }

    @Test
    void should_publish_the_reading_of_a_known_temperature_sensor() {
        when(temperaturePublisher.publish(anyDouble(), any(), any())).thenReturn(Mono.empty());
        databaseService.enqueue(json(HttpStatus.OK, "{\"point\":18,\"type\":\"temperature sensor\",\"room\":\"kitchen\"}"));

        sut.handle(FRAME).as(StepVerifier::create).verifyComplete();

        verify(temperaturePublisher).publish(24.0, RoomName.KITCHEN, RECEIVED_AT);
        assertThat(receiverLog.list).extracting(ILoggingEvent::getLevel).containsExactly(Level.INFO);
    }

    @Test
    void should_warn_when_database_service_names_the_unknown_data_point() {
        databaseService.enqueue(json(HttpStatus.NOT_FOUND, """
            {"errors":[{"message":"Device configuration not found",
                        "details":"Device not found for point: 18 on gateway: blinds",
                        "code":"NOT_FOUND_DEVICE_CONFIGURATION"}]}
            """));

        sut.handle(FRAME).as(StepVerifier::create).verifyComplete();

        assertThat(receiverLog.list).extracting(ILoggingEvent::getLevel).containsExactly(Level.INFO, Level.WARN);
        assertThat(receiverLog.list.get(1).getFormattedMessage())
            .endsWith("lookup failed (404): Device not found for point: 18 on gateway: blinds, database-service answered 404");
        verifyNoInteractions(temperaturePublisher);
    }

    @Test
    void should_log_an_error_when_the_404_carries_no_code() {
        databaseService.enqueue(json(HttpStatus.NOT_FOUND, "{\"errors\":[{\"message\":\"404 NOT_FOUND\"}]}"));

        sut.handle(FRAME).as(StepVerifier::create).verifyComplete();

        assertThat(receiverLog.list).extracting(ILoggingEvent::getLevel).containsExactly(Level.INFO, Level.ERROR);
        assertThat(receiverLog.list.get(1).getFormattedMessage())
            .endsWith("lookup failed (502): database-service answered 404 without the code NOT_FOUND_DEVICE_CONFIGURATION"
                + " - downstream: 404 NOT_FOUND");
    }

    @Test
    void should_log_an_error_with_the_words_of_a_failing_database_service() {
        databaseService.enqueue(json(HttpStatus.INTERNAL_SERVER_ERROR, """
            {"errors":[{"message":"Internal error","details":"bad SQL grammar [SELECT * FROM eaton_configuration]"}]}
            """));

        sut.handle(FRAME).as(StepVerifier::create).verifyComplete();

        assertThat(receiverLog.list).extracting(ILoggingEvent::getLevel).containsExactly(Level.INFO, Level.ERROR);
        assertThat(receiverLog.list.get(1).getFormattedMessage())
            .contains("database-service answered 500")
            .contains("bad SQL grammar");
    }

    @Test
    void should_log_an_error_when_database_service_answers_without_a_configuration() {
        //another application under the name, a route that swallows the request: 200 and nothing in it
        databaseService.enqueue(new MockResponse.Builder().code(HttpStatus.OK.value()).build());

        sut.handle(FRAME).as(StepVerifier::create).verifyComplete();

        assertThat(receiverLog.list).extracting(ILoggingEvent::getLevel).containsExactly(Level.INFO, Level.ERROR);
        verifyNoInteractions(temperaturePublisher);
    }

    @Test
    void should_log_an_error_when_the_answer_is_json_but_no_configuration() {
        databaseService.enqueue(json(HttpStatus.OK, "{}"));

        sut.handle(FRAME).as(StepVerifier::create).verifyComplete();

        assertThat(receiverLog.list).extracting(ILoggingEvent::getLevel).containsExactly(Level.INFO, Level.ERROR);
        assertThat(receiverLog.list.get(1).getFormattedMessage())
            .endsWith("lookup failed (502): database-service answered without a configuration");
        verifyNoInteractions(temperaturePublisher);
    }

    @Test
    void should_log_an_error_when_database_service_does_not_answer_in_time() {
        databaseService.enqueue(new MockResponse.Builder()
            .code(HttpStatus.OK.value())
            .addHeader(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE)
            .body("{\"point\":18,\"type\":\"temperature sensor\",\"room\":\"entrance\"}")
            .headersDelay(RESPONSE_TIMEOUT.plusSeconds(1).toMillis(), TimeUnit.MILLISECONDS)
            .build());

        sut.handle(FRAME).as(StepVerifier::create).expectComplete().verify(RESPONSE_TIMEOUT.plusSeconds(5));

        assertThat(receiverLog.list).extracting(ILoggingEvent::getLevel).containsExactly(Level.INFO, Level.ERROR);
        assertThat(receiverLog.list.get(1).getFormattedMessage())
            .endsWith("lookup failed (504): database-service did not answer within PT3S");
    }

    private static MockResponse json(final HttpStatus status, final String body) {
        return new MockResponse.Builder()
            .code(status.value())
            .addHeader(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE)
            .body(body)
            .build();
    }

    private static Logger receiverLogger() {
        return (Logger) LoggerFactory.getLogger(EatonFrameReceiver.class);
    }
}

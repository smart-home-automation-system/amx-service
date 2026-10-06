package cloud.cholewa.amx.api;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import cloud.cholewa.amx.config.ExceptionHandlerConfig;
import cloud.cholewa.amx.device.client.DeviceDatabaseClient;
import cloud.cholewa.amx.infrastructure.error.processor.ConfigurationCallExceptionProcessor;
import cloud.cholewa.amx.rabbit.TemperaturePublisher;
import cloud.cholewa.amx.service.AmxService;
import cloud.cholewa.home.model.EatonDatagramReply;
import cloud.cholewa.home.model.EatonGatewayType;
import lombok.SneakyThrows;
import mockwebserver3.Dispatcher;
import mockwebserver3.MockResponse;
import mockwebserver3.MockWebServer;
import mockwebserver3.RecordedRequest;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.webflux.test.autoconfigure.WebFluxTest;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.reactive.server.WebTestClient;
import org.springframework.web.reactive.function.BodyInserters;
import org.springframework.web.reactive.function.client.WebClient;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.verifyNoInteractions;

//The whole way of a failed lookup - what database-service answers on one side, the HTTP status the AMX
//controller gets and the level of the log line on the other. AmxControllerTest stubs the service and
//DeviceDatabaseClientTest stops at the exception, so neither shows that the code in a downstream body
//decides the status of the response, and nothing else shows the level the alerts depend on.
@Import({
    ExceptionHandlerConfig.class,
    AmxService.class,
    DeviceDatabaseClient.class,
    AmxConfigurationLookupTest.DatabaseServiceStub.class
})
@WebFluxTest(AmxController.class)
class AmxConfigurationLookupTest {

    private static final MockWebServer DATABASE_SERVICE = new MockWebServer();

    //for every test of the class, so not a few hundred milliseconds: the first call of a fresh
    //WebClient initialises Netty, and on a CI runner that alone must not turn an answer into a 504.
    //Above the 2 s DownstreamErrors gives an error body
    private static final Duration RESPONSE_TIMEOUT = Duration.ofSeconds(3);

    private final ListAppender<ILoggingEvent> processorLog = new ListAppender<>();

    @Autowired
    private WebTestClient webTestClient;

    @MockitoBean
    private TemperaturePublisher temperaturePublisher;

    @SneakyThrows
    @BeforeAll
    static void startDatabaseService() {
        DATABASE_SERVICE.start();
    }

    @SneakyThrows
    @AfterAll
    static void stopDatabaseService() {
        DATABASE_SERVICE.close();
    }

    @BeforeEach
    void captureTheLog() {
        processorLog.start();
        processorLogger().addAppender(processorLog);
    }

    @AfterEach
    void releaseTheLog() {
        processorLogger().detachAppender(processorLog);
    }

    @Test
    void should_answer_404_and_warn_when_database_service_names_the_unknown_data_point() {
        databaseServiceAnswers(json(HttpStatus.NOT_FOUND, """
            {"errors":[{"message":"Device configuration not found",
                        "details":"Device not found for point: 18 on gateway: blinds",
                        "code":"NOT_FOUND_DEVICE_CONFIGURATION"}]}
            """));

        postDatagram()
            .expectStatus().isNotFound()
            .expectBody()
            .jsonPath("$.errors[0].details")
            .isEqualTo("Device not found for point: 18 on gateway: blinds, database-service answered 404");

        assertThat(processorLog.list).extracting(ILoggingEvent::getLevel).containsExactly(Level.WARN);
    }

    //the point of HAS-176: the answer of a path nothing is mapped to - a renamed endpoint, not an unknown
    //data point - has to reach the alerts, and they look at ERROR
    @Test
    void should_answer_502_and_log_an_error_when_the_404_carries_no_code() {
        databaseServiceAnswers(json(HttpStatus.NOT_FOUND, "{\"errors\":[{\"message\":\"404 NOT_FOUND\"}]}"));

        postDatagram()
            .expectStatus().isEqualTo(HttpStatus.BAD_GATEWAY)
            .expectBody()
            .jsonPath("$.errors[0].details")
            .isEqualTo("database-service answered 404 without the code NOT_FOUND_DEVICE_CONFIGURATION");

        assertThat(processorLog.list).extracting(ILoggingEvent::getLevel).containsExactly(Level.ERROR);
    }

    @Test
    void should_answer_502_without_the_words_of_a_failing_database_service() {
        databaseServiceAnswers(json(HttpStatus.INTERNAL_SERVER_ERROR, """
            {"errors":[{"message":"Internal error","details":"bad SQL grammar [SELECT * FROM eaton_configuration]"}]}
            """));

        postDatagram()
            .expectStatus().isEqualTo(HttpStatus.BAD_GATEWAY)
            .expectBody()
            .jsonPath("$.errors.length()").isEqualTo(1)
            .jsonPath("$.errors[0].details").isEqualTo("database-service answered 500");

        //the log is where the cause belongs
        assertThat(processorLog.list).singleElement().satisfies(event -> {
            assertThat(event.getLevel()).isEqualTo(Level.ERROR);
            assertThat(event.getFormattedMessage()).contains("bad SQL grammar");
        });
    }

    @Test
    void should_answer_502_when_database_service_answers_without_a_configuration() {
        //another application under the name, a route that swallows the request: 200 and nothing in it
        databaseServiceAnswers(new MockResponse.Builder().code(HttpStatus.OK.value()).build());

        postDatagram().expectStatus().isEqualTo(HttpStatus.BAD_GATEWAY);

        assertThat(processorLog.list).extracting(ILoggingEvent::getLevel).containsExactly(Level.ERROR);
        verifyNoInteractions(temperaturePublisher);
    }

    @Test
    void should_answer_502_when_the_configuration_cannot_be_read() {
        databaseServiceAnswers(json(HttpStatus.OK, "{\"point\":18,\"type\":\"teleporter\",\"room\":\"entrance\"}"));

        postDatagram()
            .expectStatus().isEqualTo(HttpStatus.BAD_GATEWAY)
            .expectBody()
            //the type of the failure only: a decoding error quotes the body
            .jsonPath("$.errors[0].details").value(details -> assertThat((String) details)
                .startsWith("unreadable answer of database-service: ")
                .doesNotContain("teleporter"));

        assertThat(processorLog.list).extracting(ILoggingEvent::getLevel).containsExactly(Level.ERROR);
    }

    @Test
    void should_answer_502_when_the_answer_is_json_but_no_configuration() {
        //what any other application says to a path it does not know, with a 200 in front of it
        databaseServiceAnswers(json(HttpStatus.OK, "{}"));

        postDatagram()
            .expectStatus().isEqualTo(HttpStatus.BAD_GATEWAY)
            .expectBody()
            .jsonPath("$.errors[0].details").isEqualTo("database-service answered without a configuration");

        assertThat(processorLog.list).extracting(ILoggingEvent::getLevel).containsExactly(Level.ERROR);
        verifyNoInteractions(temperaturePublisher);
    }

    @Test
    void should_answer_504_when_database_service_does_not_answer_in_time() {
        databaseServiceAnswers(new MockResponse.Builder()
            .code(HttpStatus.OK.value())
            .addHeader(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE)
            .body("{\"point\":18,\"type\":\"temperature sensor\",\"room\":\"entrance\"}")
            .headersDelay(RESPONSE_TIMEOUT.plusSeconds(1).toMillis(), TimeUnit.MILLISECONDS)
            .build());

        postDatagram().expectStatus().isEqualTo(HttpStatus.GATEWAY_TIMEOUT);

        assertThat(processorLog.list).extracting(ILoggingEvent::getLevel).containsExactly(Level.ERROR);
    }

    private WebTestClient.ResponseSpec postDatagram() {
        //well above the timeout of the lookup: the 504 has to arrive as an answer, not as a test that gave up
        return webTestClient.mutate().responseTimeout(Duration.ofSeconds(15)).build().post()
            .uri("/amx")
            .body(BodyInserters.fromValue(EatonDatagramReply.builder()
                .gateway(EatonGatewayType.BLINDS)
                .message("5A,C,C1,12,70,32,10,00,00,00,00,00,00,A5")
                .build()))
            .exchange();
    }

    private static MockResponse json(final HttpStatus status, final String body) {
        return new MockResponse.Builder()
            .code(status.value())
            .addHeader(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE)
            .body(body)
            .build();
    }

    //a dispatcher, not the queue: the server lives as long as the class, and a response one test left
    //unread in a queue would be served to the next
    private static void databaseServiceAnswers(final MockResponse response) {
        DATABASE_SERVICE.setDispatcher(new Dispatcher() {
            @Override
            public MockResponse dispatch(final RecordedRequest request) {
                return response;
            }
        });
    }

    private static Logger processorLogger() {
        return (Logger) LoggerFactory.getLogger(ConfigurationCallExceptionProcessor.class);
    }

    @DynamicPropertySource
    static void pointAtTheStub(final DynamicPropertyRegistry registry) {
        registry.add("internal.service.database.host", () -> "localhost");
        //read when the context is built, which is after the server got its port
        registry.add("internal.service.database.port", DATABASE_SERVICE::getPort);
        registry.add("internal.service.database.response-timeout", () -> RESPONSE_TIMEOUT);
    }

    @TestConfiguration
    static class DatabaseServiceStub {

        @Bean
        WebClient webClient() {
            return WebClient.create();
        }
    }
}

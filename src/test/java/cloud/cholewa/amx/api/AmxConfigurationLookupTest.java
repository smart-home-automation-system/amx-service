package cloud.cholewa.amx.api;

import cloud.cholewa.amx.config.ExceptionHandlerConfig;
import cloud.cholewa.amx.device.client.DeviceDatabaseClient;
import cloud.cholewa.amx.rabbit.TemperaturePublisher;
import cloud.cholewa.amx.service.AmxService;
import cloud.cholewa.home.model.EatonDatagramReply;
import cloud.cholewa.home.model.EatonGatewayType;
import lombok.SneakyThrows;
import mockwebserver3.MockResponse;
import mockwebserver3.MockWebServer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
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

//The whole way of a failed lookup - what database-service answers on one side, the HTTP status the AMX
//controller gets on the other. AmxControllerTest stubs the service and DeviceDatabaseClientTest stops at
//the exception, so neither shows that the code in a downstream body decides the status of the response.
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
    //Still below the 5 s WebTestClient waits for the response
    private static final Duration RESPONSE_TIMEOUT = Duration.ofSeconds(2);

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

    @Test
    void should_answer_404_when_database_service_names_the_unknown_data_point() {
        DATABASE_SERVICE.enqueue(new MockResponse.Builder()
            .code(HttpStatus.NOT_FOUND.value())
            .addHeader(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE)
            .body("""
                {"errors":[{"message":"Device configuration not found",
                            "details":"Device not found for point: 18 on gateway: blinds",
                            "code":"NOT_FOUND_DEVICE_CONFIGURATION"}]}
                """)
            .build()
        );

        postDatagram()
            .expectStatus().isNotFound()
            .expectBody()
            .jsonPath("$.errors[0].details")
            .isEqualTo("Device not found for point: 18 on gateway: blinds, database-service answered 404");
    }

    @Test
    void should_answer_502_when_the_404_carries_no_code() {
        //the answer of a path nothing is mapped to - a renamed endpoint, not an unknown data point
        DATABASE_SERVICE.enqueue(new MockResponse.Builder()
            .code(HttpStatus.NOT_FOUND.value())
            .addHeader(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE)
            .body("{\"errors\":[{\"message\":\"404 NOT_FOUND\"}]}")
            .build()
        );

        postDatagram()
            .expectStatus().isEqualTo(HttpStatus.BAD_GATEWAY)
            .expectBody()
            .jsonPath("$.errors[0].details")
            .isEqualTo("404 NOT_FOUND, database-service answered 404 without the code NOT_FOUND_DEVICE_CONFIGURATION");
    }

    @Test
    void should_answer_502_when_database_service_fails() {
        DATABASE_SERVICE.enqueue(new MockResponse.Builder()
            .code(HttpStatus.INTERNAL_SERVER_ERROR.value())
            .addHeader(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE)
            .body("{\"errors\":[{\"message\":\"boom\"}]}")
            .build()
        );

        postDatagram().expectStatus().isEqualTo(HttpStatus.BAD_GATEWAY);
    }

    @Test
    void should_answer_504_when_database_service_does_not_answer_in_time() {
        DATABASE_SERVICE.enqueue(new MockResponse.Builder()
            .code(HttpStatus.OK.value())
            .addHeader(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE)
            .body("{\"point\":18,\"type\":\"temperature sensor\",\"room\":\"entrance\"}")
            .headersDelay(RESPONSE_TIMEOUT.multipliedBy(2).toMillis(), TimeUnit.MILLISECONDS)
            .build()
        );

        postDatagram().expectStatus().isEqualTo(HttpStatus.GATEWAY_TIMEOUT);
    }

    private WebTestClient.ResponseSpec postDatagram() {
        return webTestClient.post()
            .uri("/amx")
            .body(BodyInserters.fromValue(EatonDatagramReply.builder()
                .gateway(EatonGatewayType.BLINDS)
                .message("5A,C,C1,12,70,32,10,00,00,00,00,00,00,A5")
                .build()))
            .exchange();
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

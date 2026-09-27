package cloud.cholewa.amx.device.client;

import cloud.cholewa.amx.infrastructure.error.ConfigurationCallException;
import cloud.cholewa.commons.error.model.ErrorMessage;
import cloud.cholewa.home.model.EatonGatewayType;
import cloud.cholewa.home.model.RoomName;
import cloud.cholewa.home.model.SmartDeviceType;
import lombok.SneakyThrows;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.test.StepVerifier;

import java.net.ServerSocket;
import java.time.Duration;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

@ExtendWith(MockitoExtension.class)
class DeviceDatabaseClientTest {

    private MockWebServer mockWebServer;
    private DeviceDatabaseClient sut;

    @SneakyThrows
    @BeforeEach
    void setUp() {
        mockWebServer = new MockWebServer();
        mockWebServer.start(3000);

        sut = clientWithTimeout(Duration.ofSeconds(5));
    }

    @SneakyThrows
    @AfterEach
    void tearDown() {
        mockWebServer.shutdown();
    }

    @Test
    void should_return_exception__when_device_configuration_not_found() {
        mockWebServer.enqueue(new MockResponse()
            .setResponseCode(HttpStatus.NOT_FOUND.value())
            .addHeader(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE)
            .setBody("""
                {
                  "errors": [
                    {
                      "message": "dummy message",
                      "details": "dummy details"
                    }
                  ]
                }
                """)
        );

        sut.getEatonConfiguration(56, EatonGatewayType.BLINDS)
            .as(StepVerifier::create)
            .expectError(ConfigurationCallException.class)
            .verify();
    }

    @Test
    void should_return_gateway_timeout__when_database_service_does_not_answer_in_time() {
        mockWebServer.enqueue(new MockResponse()
            .setResponseCode(HttpStatus.OK.value())
            .addHeader(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE)
            .setBody("{\"point\":56,\"type\":\"temperature sensor\",\"room\":\"entrance\"}")
            .setHeadersDelay(2, TimeUnit.SECONDS)
        );

        //a short bound only here: the first call of a fresh WebClient initialises Netty, which on a CI
        //runner with JaCoCo instrumentation alone can take longer than this
        clientWithTimeout(Duration.ofMillis(200)).getEatonConfiguration(56, EatonGatewayType.BLINDS)
            .as(StepVerifier::create)
            .expectErrorSatisfies(throwable -> assertThat(throwable)
                .isInstanceOfSatisfying(ConfigurationCallException.class, exception ->
                    assertThat(exception.getHttpStatus()).isEqualTo(HttpStatus.GATEWAY_TIMEOUT)))
            .verify(Duration.ofSeconds(3));
    }

    @Test
    void should_carry_the_downstream_4xx_status_and_details() {
        mockWebServer.enqueue(new MockResponse()
            .setResponseCode(HttpStatus.NOT_FOUND.value())
            .addHeader(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE)
            //what database-service really sends: the status is in the response, not in the body
            .setBody("{\"errors\":[{\"message\":\"Device configuration not found\",\"details\":\"point 71\"}]}")
        );

        sut.getEatonConfiguration(71, EatonGatewayType.BLINDS)
            .as(StepVerifier::create)
            .expectErrorSatisfies(throwable -> assertThat(throwable)
                .isInstanceOfSatisfying(ConfigurationCallException.class, exception -> {
                    assertThat(exception.getHttpStatus()).isEqualTo(HttpStatus.NOT_FOUND);
                    assertThat(exception.getErrorMessages()).extracting(ErrorMessage::getDetails)
                        .containsExactly("point 71", "database-service answered 404");
                }))
            .verify();
    }

    @Test
    void should_report_a_downstream_5xx_as_bad_gateway() {
        mockWebServer.enqueue(new MockResponse()
            .setResponseCode(HttpStatus.INTERNAL_SERVER_ERROR.value())
            .addHeader(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE)
            .setBody("{\"errors\":[{\"message\":\"boom\"}]}")
        );

        sut.getEatonConfiguration(56, EatonGatewayType.BLINDS)
            .as(StepVerifier::create)
            .expectErrorSatisfies(throwable -> assertThat(throwable)
                .isInstanceOfSatisfying(ConfigurationCallException.class, exception ->
                    assertThat(exception.getHttpStatus()).isEqualTo(HttpStatus.BAD_GATEWAY)))
            .verify();
    }

    @Test
    void should_report_a_downstream_4xx_other_than_404_as_bad_gateway() {
        //e.g. database-service rejecting a query amx-service built - not the AMX controller's fault
        mockWebServer.enqueue(new MockResponse()
            .setResponseCode(HttpStatus.BAD_REQUEST.value())
            .addHeader(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE)
            .setBody("{\"errors\":[{\"message\":\"Unknown Eaton gateway\",\"details\":\"garden\"}]}")
        );

        sut.getEatonConfiguration(56, EatonGatewayType.BLINDS)
            .as(StepVerifier::create)
            .expectErrorSatisfies(throwable -> assertThat(throwable)
                .isInstanceOfSatisfying(ConfigurationCallException.class, exception ->
                    assertThat(exception.getHttpStatus()).isEqualTo(HttpStatus.BAD_GATEWAY)))
            .verify();
    }

    @Test
    void should_report_an_unreachable_database_service_as_bad_gateway() {
        //a port that was free a moment ago - nothing listens there, the connect is refused. 127.0.0.1, not
        //localhost: on Windows a refused connect is retried per resolved address, which takes seconds; the
        //response timeout stays far above the verify window so a slow refusal cannot turn into a 504
        new DeviceDatabaseClient(WebClient.create(), new DeviceDatabaseClientConfig("127.0.0.1", freePort(), Duration.ofSeconds(60)))
            .getEatonConfiguration(56, EatonGatewayType.BLINDS)
            .as(StepVerifier::create)
            .expectErrorSatisfies(throwable -> assertThat(throwable)
                .isInstanceOfSatisfying(ConfigurationCallException.class, exception ->
                    assertThat(exception.getHttpStatus()).isEqualTo(HttpStatus.BAD_GATEWAY)))
            .verify(Duration.ofSeconds(20));
    }

    @SneakyThrows
    private static String freePort() {
        try (ServerSocket socket = new ServerSocket(0)) {
            return String.valueOf(socket.getLocalPort());
        }
    }

    @Test
    void should_keep_the_status_when_the_error_body_is_not_the_errors_contract() {
        mockWebServer.enqueue(new MockResponse()
            .setResponseCode(HttpStatus.SERVICE_UNAVAILABLE.value())
            .addHeader(HttpHeaders.CONTENT_TYPE, MediaType.TEXT_HTML_VALUE)
            .setBody("<html>upstream unavailable</html>")
        );

        sut.getEatonConfiguration(56, EatonGatewayType.BLINDS)
            .as(StepVerifier::create)
            .expectErrorSatisfies(throwable -> assertThat(throwable)
                .isInstanceOfSatisfying(ConfigurationCallException.class, exception -> {
                    assertThat(exception.getHttpStatus()).isEqualTo(HttpStatus.BAD_GATEWAY);
                    assertThat(exception.getErrorMessages()).extracting(ErrorMessage::getDetails)
                        .containsExactly("database-service answered 503");
                }))
            .verify();
    }

    @Test
    void should_return_device_configuration() {
        mockWebServer.enqueue(new MockResponse()
            .setResponseCode(HttpStatus.OK.value())
            .addHeader("Content-Type", "application/json")
            .setBody("{\"point\":56,\"type\":\"temperature sensor\",\"room\":\"entrance\"}")
        );

        sut.getEatonConfiguration(56, EatonGatewayType.BLINDS)
            .as(StepVerifier::create)
            .assertNext(eatonConfigurationResponse -> {
                assertThat(eatonConfigurationResponse.getPoint()).isEqualTo(56);
                assertThat(eatonConfigurationResponse.getType()).isEqualTo(SmartDeviceType.TEMPERATURE_SENSOR);
                assertThat(eatonConfigurationResponse.getRoom()).isEqualTo(RoomName.ENTRANCE);
            })
            .verifyComplete();
    }

    private static DeviceDatabaseClient clientWithTimeout(final Duration responseTimeout) {
        return new DeviceDatabaseClient(
            WebClient.create(),
            new DeviceDatabaseClientConfig("localhost", "3000", responseTimeout)
        );
    }
}

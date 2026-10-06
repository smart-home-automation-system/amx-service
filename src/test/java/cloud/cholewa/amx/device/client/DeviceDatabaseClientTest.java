package cloud.cholewa.amx.device.client;

import cloud.cholewa.amx.infrastructure.error.ConfigurationCallException;
import cloud.cholewa.commons.error.model.ErrorMessage;
import cloud.cholewa.home.model.EatonGatewayType;
import cloud.cholewa.home.model.RoomName;
import cloud.cholewa.home.model.SmartDeviceType;
import lombok.SneakyThrows;
import mockwebserver3.MockResponse;
import mockwebserver3.MockWebServer;
import mockwebserver3.SocketEffect;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
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
        mockWebServer.close();
    }

    @Test
    void should_relay_a_404_that_names_the_unknown_data_point() {
        mockWebServer.enqueue(new MockResponse.Builder()
            .code(HttpStatus.NOT_FOUND.value())
            .addHeader(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE)
            //what database-service really sends since 0.8.0: the status is in the response, not in the body
            .body("""
                {"errors":[{"message":"Device configuration not found",
                            "details":"Device not found for point: 71 on gateway: blinds",
                            "code":"NOT_FOUND_DEVICE_CONFIGURATION"}]}
                """)
            .build()
        );

        sut.getEatonConfiguration(71, EatonGatewayType.BLINDS)
            .as(StepVerifier::create)
            .expectErrorSatisfies(throwable -> assertThat(throwable)
                .isInstanceOfSatisfying(ConfigurationCallException.class, exception -> {
                    assertThat(exception.getHttpStatus()).isEqualTo(HttpStatus.NOT_FOUND);
                    assertThat(exception.getErrorMessages()).extracting(ErrorMessage::getDetails)
                        .containsExactly(
                            "Device not found for point: 71 on gateway: blinds",
                            "database-service answered 404"
                        );
                }))
            .verify();
    }

    //a 404 alone does not say what is missing: the data point, or the path it was asked under
    @ParameterizedTest
    @ValueSource(strings = {
        //the Errors contract of a database-service before 0.8.0, or of another cause
        "{\"errors\":[{\"message\":\"Device configuration not found\",\"details\":\"point 71\"}]}",
        "{\"errors\":[{\"message\":\"No such member\",\"code\":\"NOT_FOUND_HOUSEHOLD_MEMBER\"}]}",
        //Spring's own answer for a path nothing is mapped to
        "{\"timestamp\":\"2026-10-06T10:00:00Z\",\"path\":\"/home/device/configuration/eaton\",\"status\":404,\"error\":\"Not Found\"}",
        "<html>404 page not found</html>",
        ""
    })
    void should_report_a_404_without_the_code_as_bad_gateway(final String body) {
        mockWebServer.enqueue(new MockResponse.Builder()
            .code(HttpStatus.NOT_FOUND.value())
            .addHeader(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE)
            .body(body)
            .build()
        );

        sut.getEatonConfiguration(71, EatonGatewayType.BLINDS)
            .as(StepVerifier::create)
            .expectErrorSatisfies(throwable -> assertThat(throwable)
                .isInstanceOfSatisfying(ConfigurationCallException.class, exception -> {
                    assertThat(exception.getHttpStatus()).isEqualTo(HttpStatus.BAD_GATEWAY);
                    assertThat(exception.getErrorMessages()).extracting(ErrorMessage::getDetails)
                        .contains("database-service answered 404 without the code NOT_FOUND_DEVICE_CONFIGURATION");
                }))
            .verify();
    }

    @Test
    void should_not_relay_the_code_of_another_status() {
        //the code names the cause of a 404; under any other status it is not the answer to this lookup
        mockWebServer.enqueue(new MockResponse.Builder()
            .code(HttpStatus.INTERNAL_SERVER_ERROR.value())
            .addHeader(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE)
            .body("{\"errors\":[{\"message\":\"boom\",\"code\":\"NOT_FOUND_DEVICE_CONFIGURATION\"}]}")
            .build()
        );

        sut.getEatonConfiguration(71, EatonGatewayType.BLINDS)
            .as(StepVerifier::create)
            .expectErrorSatisfies(throwable -> assertThat(throwable)
                .isInstanceOfSatisfying(ConfigurationCallException.class, exception ->
                    assertThat(exception.getHttpStatus()).isEqualTo(HttpStatus.BAD_GATEWAY)))
            .verify();
    }

    @Test
    void should_return_gateway_timeout__when_database_service_does_not_answer_in_time() {
        mockWebServer.enqueue(new MockResponse.Builder()
            .code(HttpStatus.OK.value())
            .addHeader(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE)
            .body("{\"point\":56,\"type\":\"temperature sensor\",\"room\":\"entrance\"}")
            .headersDelay(2, TimeUnit.SECONDS)
            .build()
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
    void should_report_a_downstream_5xx_as_bad_gateway() {
        mockWebServer.enqueue(new MockResponse.Builder()
            .code(HttpStatus.INTERNAL_SERVER_ERROR.value())
            .addHeader(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE)
            .body("{\"errors\":[{\"message\":\"boom\",\"details\":\"bad SQL grammar\"}]}")
            .build()
        );

        sut.getEatonConfiguration(56, EatonGatewayType.BLINDS)
            .as(StepVerifier::create)
            .expectErrorSatisfies(throwable -> assertThat(throwable)
                .isInstanceOfSatisfying(ConfigurationCallException.class, exception -> {
                    assertThat(exception.getHttpStatus()).isEqualTo(HttpStatus.BAD_GATEWAY);
                    //what a failing database-service says is for the log, not for the answer
                    assertThat(exception.getErrorMessages()).extracting(ErrorMessage::getDetails)
                        .containsExactly("database-service answered 500");
                    assertThat(exception.getDownstreamMessages()).extracting(ErrorMessage::getDetails)
                        .containsExactly("bad SQL grammar");
                }))
            .verify();
    }

    @Test
    void should_report_an_answer_without_a_configuration_as_bad_gateway() {
        mockWebServer.enqueue(new MockResponse.Builder().code(HttpStatus.NO_CONTENT.value()).build());

        sut.getEatonConfiguration(56, EatonGatewayType.BLINDS)
            .as(StepVerifier::create)
            .expectErrorSatisfies(throwable -> assertThat(throwable)
                .isInstanceOfSatisfying(ConfigurationCallException.class, exception ->
                    assertThat(exception.getHttpStatus()).isEqualTo(HttpStatus.BAD_GATEWAY)))
            .verify();
    }

    @Test
    void should_report_a_connection_lost_in_the_middle_of_the_answer_as_bad_gateway() {
        //the headers arrived, so this is no WebClientRequestException any more
        mockWebServer.enqueue(new MockResponse.Builder()
            .code(HttpStatus.OK.value())
            .addHeader(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE)
            .body("{\"point\":56,\"type\":\"temperature sensor\",\"room\":\"entrance\"}")
            .onResponseBody(new SocketEffect.CloseSocket())
            .build()
        );

        sut.getEatonConfiguration(56, EatonGatewayType.BLINDS)
            .as(StepVerifier::create)
            .expectErrorSatisfies(throwable -> assertThat(throwable)
                .isInstanceOfSatisfying(ConfigurationCallException.class, exception ->
                    assertThat(exception.getHttpStatus()).isEqualTo(HttpStatus.BAD_GATEWAY)))
            .verify();
    }

    @Test
    void should_report_an_answer_that_does_not_decode_as_bad_gateway() {
        mockWebServer.enqueue(new MockResponse.Builder()
            .code(HttpStatus.OK.value())
            .addHeader(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE)
            .body("{\"point\":56,\"type\":\"teleporter\",\"room\":\"entrance\"}")
            .build()
        );

        sut.getEatonConfiguration(56, EatonGatewayType.BLINDS)
            .as(StepVerifier::create)
            .expectErrorSatisfies(throwable -> assertThat(throwable)
                .isInstanceOfSatisfying(ConfigurationCallException.class, exception -> {
                    assertThat(exception.getHttpStatus()).isEqualTo(HttpStatus.BAD_GATEWAY);
                    assertThat(exception.getErrorMessages()).extracting(ErrorMessage::getDetails)
                        .allSatisfy(details -> assertThat(details).doesNotContain("teleporter"));
                }))
            .verify();
    }

    @Test
    void should_report_a_downstream_4xx_other_than_404_as_bad_gateway() {
        //e.g. database-service rejecting a query amx-service built - not the AMX controller's fault
        mockWebServer.enqueue(new MockResponse.Builder()
            .code(HttpStatus.BAD_REQUEST.value())
            .addHeader(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE)
            .body("{\"errors\":[{\"message\":\"Unknown Eaton gateway\",\"details\":\"garden\"}]}")
            .build()
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
        mockWebServer.enqueue(new MockResponse.Builder()
            .code(HttpStatus.SERVICE_UNAVAILABLE.value())
            .addHeader(HttpHeaders.CONTENT_TYPE, MediaType.TEXT_HTML_VALUE)
            .body("<html>upstream unavailable</html>")
            .build()
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
        mockWebServer.enqueue(new MockResponse.Builder()
            .code(HttpStatus.OK.value())
            .addHeader("Content-Type", "application/json")
            .body("{\"point\":56,\"type\":\"temperature sensor\",\"room\":\"entrance\"}")
            .build()
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

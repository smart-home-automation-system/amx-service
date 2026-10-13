package cloud.cholewa.amx.device.client;

import cloud.cholewa.amx.infrastructure.error.ConfigurationCallException;
import cloud.cholewa.commons.error.model.ErrorMessage;
import cloud.cholewa.home.model.DeviceKind;
import cloud.cholewa.home.model.EatonGatewayType;
import cloud.cholewa.home.model.LightKind;
import cloud.cholewa.home.model.SmartDevice;
import lombok.SneakyThrows;
import mockwebserver3.MockResponse;
import mockwebserver3.MockWebServer;
import mockwebserver3.RecordedRequest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.client.reactive.ReactorClientHttpConnector;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.netty.http.client.HttpClient;
import reactor.test.StepVerifier;

import java.net.ServerSocket;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;

class DeviceRegistryClientTest {

    private static final Duration PATIENCE = Duration.ofSeconds(20);

    private MockWebServer mockWebServer;
    private DeviceRegistryClient sut;

    @SneakyThrows
    @BeforeEach
    void setUp() {
        //a port of its own and a client without a pool, as in DeviceDatabaseClientTest (HAS-216)
        mockWebServer = new MockWebServer();
        mockWebServer.start();

        sut = clientWithTimeout(Duration.ofSeconds(5));
    }

    @SneakyThrows
    @AfterEach
    void tearDown() {
        mockWebServer.close();
    }

    @SneakyThrows
    @Test
    void should_ask_the_registry_for_the_kind_by_its_value_and_read_the_devices() {
        //what database-service answers: a Shelly light next to the Eaton ones
        mockWebServer.enqueue(json("""
            [{"id":21,"name":"Kitchen ceiling","room":"kitchen","kind":"light","enabled":true,"vendor":"eaton",
              "eaton":{"gateway":"lights","point":7},"lightKind":"dimmer"},
             {"id":22,"name":"Office lamp","room":"office","kind":"light","enabled":false,"vendor":"shelly",
              "shelly":{"address":"192.0.2.17","generation":2,"deviceId":"shellyplus1-a1b2c3","channel":0},
              "lightKind":"relay"}]
            """));

        sut.getDevices(DeviceKind.LIGHT)
            .as(StepVerifier::create)
            .assertNext(devices -> {
                assertThat(devices).hasSize(2);
                final SmartDevice eaton = devices.getFirst();
                assertThat(eaton.getName()).isEqualTo("Kitchen ceiling");
                assertThat(eaton.getEaton().getGateway()).isEqualTo(EatonGatewayType.LIGHTS);
                assertThat(eaton.getEaton().getPoint()).isEqualTo(7);
                assertThat(eaton.getLightKind()).isEqualTo(LightKind.DIMMER);
                assertThat(devices.getLast().getEaton()).isNull();
            })
            .expectComplete()
            .verify(PATIENCE);

        final RecordedRequest request = mockWebServer.takeRequest(1, TimeUnit.SECONDS);
        assertThat(request).isNotNull();
        assertThat(request.getMethod()).isEqualTo("GET");
        assertThat(request.getUrl().encodedPath()).isEqualTo("/home/device/registry");
        assertThat(request.getUrl().queryParameter("kind")).isEqualTo("light");
    }

    @Test
    void should_read_a_registry_without_a_device_of_the_kind_as_an_empty_list() {
        mockWebServer.enqueue(json("[]"));

        sut.getDevices(DeviceKind.BLINDS)
            .as(StepVerifier::create)
            .assertNext(devices -> assertThat(devices).isEmpty())
            .expectComplete()
            .verify(PATIENCE);
    }

    //what a failing database-service says is raw exception text: it goes to the log, and the answer
    //names the status alone
    @Test
    void should_report_an_error_status_as_bad_gateway_and_keep_what_was_said_for_the_log() {
        mockWebServer.enqueue(new MockResponse.Builder()
            .code(HttpStatus.INTERNAL_SERVER_ERROR.value())
            .addHeader(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE)
            .body("""
                {"errors":[{"message":"Internal error","details":"select * from devices failed"}]}
                """)
            .build());

        sut.getDevices(DeviceKind.BLINDS)
            .as(StepVerifier::create)
            .expectErrorSatisfies(throwable -> assertThat(throwable)
                .isInstanceOfSatisfying(ConfigurationCallException.class, exception -> {
                    assertThat(exception.getHttpStatus()).isEqualTo(HttpStatus.BAD_GATEWAY);
                    assertThat(exception.getErrorMessages()).extracting(ErrorMessage::getDetails)
                        .containsExactly("database-service answered 500");
                    assertThat(exception.getDownstreamMessages()).extracting(ErrorMessage::getDetails)
                        .containsExactly("select * from devices failed");
                }))
            .verify(PATIENCE);
    }

    //the registry has no "not found" for a list: a 404 is a path nothing answers under
    @Test
    void should_report_a_404_as_bad_gateway() {
        mockWebServer.enqueue(new MockResponse.Builder().code(HttpStatus.NOT_FOUND.value()).build());

        sut.getDevices(DeviceKind.BLINDS)
            .as(StepVerifier::create)
            .expectErrorSatisfies(failure(HttpStatus.BAD_GATEWAY, "database-service answered 404"))
            .verify(PATIENCE);
    }

    @Test
    void should_report_an_answer_without_a_body_as_bad_gateway() {
        mockWebServer.enqueue(new MockResponse.Builder().code(HttpStatus.OK.value()).build());

        sut.getDevices(DeviceKind.BLINDS)
            .as(StepVerifier::create)
            .expectErrorSatisfies(failure(
                HttpStatus.BAD_GATEWAY, "database-service answered without a list of devices"))
            .verify(PATIENCE);
    }

    //only the type of the failure is answered: a decoding error quotes the body
    @Test
    void should_report_an_answer_that_is_no_list_of_devices_as_bad_gateway() {
        mockWebServer.enqueue(json("""
            {"devices":"none"}
            """));

        sut.getDevices(DeviceKind.BLINDS)
            .as(StepVerifier::create)
            .expectErrorSatisfies(throwable -> assertThat(throwable)
                .isInstanceOfSatisfying(ConfigurationCallException.class, exception -> {
                    assertThat(exception.getHttpStatus()).isEqualTo(HttpStatus.BAD_GATEWAY);
                    assertThat(exception.getErrorMessages()).extracting(ErrorMessage::getDetails)
                        .singleElement().asString()
                        .startsWith("unreadable answer of database-service: ")
                        .doesNotContain("none");
                }))
            .verify(PATIENCE);
    }

    @Test
    void should_report_a_registry_that_does_not_answer_in_time_as_gateway_timeout() {
        sut = clientWithTimeout(Duration.ofMillis(300));
        mockWebServer.enqueue(new MockResponse.Builder()
            .code(HttpStatus.OK.value())
            .addHeader(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE)
            .body("[]")
            .headersDelay(3, TimeUnit.SECONDS)
            .build());

        sut.getDevices(DeviceKind.BLINDS)
            .as(StepVerifier::create)
            .expectErrorSatisfies(failure(
                HttpStatus.GATEWAY_TIMEOUT, "database-service did not answer within PT0.3S"))
            .verify(PATIENCE);
    }

    @SneakyThrows
    @Test
    void should_report_an_unreachable_database_service_as_bad_gateway() {
        final String nobody;
        try (ServerSocket socket = new ServerSocket(0)) {
            nobody = String.valueOf(socket.getLocalPort());
        }

        //127.0.0.1 and a timeout far above the verify window, as in DeviceDatabaseClientTest
        new DeviceRegistryClient(
            newConnectionClient(), new DeviceDatabaseClientConfig("127.0.0.1", nobody, Duration.ofSeconds(60)))
            .getDevices(DeviceKind.BLINDS)
            .as(StepVerifier::create)
            .expectErrorSatisfies(throwable -> assertThat(throwable)
                .isInstanceOfSatisfying(ConfigurationCallException.class, exception ->
                    assertThat(exception.getHttpStatus()).isEqualTo(HttpStatus.BAD_GATEWAY)))
            .verify(PATIENCE);
    }

    private static MockResponse json(final String body) {
        return new MockResponse.Builder()
            .code(HttpStatus.OK.value())
            .addHeader(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE)
            .body(body)
            .build();
    }

    private static Consumer<Throwable> failure(final HttpStatus status, final String details) {
        return throwable -> assertThat(throwable)
            .isInstanceOfSatisfying(ConfigurationCallException.class, exception -> {
                assertThat(exception.getHttpStatus()).isEqualTo(status);
                assertThat(exception.getErrorMessages()).extracting(ErrorMessage::getDetails)
                    .containsExactly(details);
            });
    }

    private DeviceRegistryClient clientWithTimeout(final Duration responseTimeout) {
        return new DeviceRegistryClient(
            newConnectionClient(),
            new DeviceDatabaseClientConfig("localhost", String.valueOf(mockWebServer.getPort()), responseTimeout)
        );
    }

    private static WebClient newConnectionClient() {
        return WebClient.builder()
            .clientConnector(new ReactorClientHttpConnector(HttpClient.newConnection()))
            .build();
    }
}

package cloud.cholewa.amx.device.client;

import cloud.cholewa.amx.infrastructure.error.ConfigurationCallException;
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

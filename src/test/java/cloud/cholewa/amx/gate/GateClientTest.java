package cloud.cholewa.amx.gate;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import lombok.SneakyThrows;
import mockwebserver3.MockResponse;
import mockwebserver3.MockWebServer;
import mockwebserver3.RecordedRequest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.client.reactive.ReactorClientHttpConnector;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.netty.http.client.HttpClient;
import reactor.test.StepVerifier;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

//The gate opened for the entrance panel (HAS-487): what gate-service answers against what the panel
//is told - and the level of the log line, which is what the alerts see
class GateClientTest {

    private static final Duration PATIENCE = Duration.ofSeconds(10);

    private final ListAppender<ILoggingEvent> log = new ListAppender<>();
    private MockWebServer server;
    private GateClient sut;

    @SneakyThrows
    @BeforeEach
    void setUp() {
        server = new MockWebServer();
        server.start();
        sut = client(Duration.ofSeconds(5));
        log.start();
        ((Logger) LoggerFactory.getLogger(GateClient.class)).addAppender(log);
    }

    @SneakyThrows
    @AfterEach
    void tearDown() {
        ((Logger) LoggerFactory.getLogger(GateClient.class)).detachAppender(log);
        server.close();
    }

    @SneakyThrows
    @Test
    void should_open_the_gate_with_the_source_panel() {
        server.enqueue(new MockResponse.Builder().code(204).build());

        expectOpening(true);

        final RecordedRequest request = server.takeRequest(5, TimeUnit.SECONDS);
        assertThat(request).isNotNull();
        assertThat(request.getMethod()).isEqualTo("POST");
        assertThat(request.getUrl().encodedPath()).isEqualTo("/home/gate/open");
        assertThat(request.getUrl().queryParameter("source")).isEqualTo("panel");
        assertThat(request.getUrl().queryParameter("member")).isNull();
        assertThat(warnings()).isEmpty();
    }

    @Test
    void should_take_an_opening_of_the_last_seconds_for_a_gate_that_is_opening() {
        server.enqueue(error(409, "OPEN_TOO_SOON"));

        expectOpening(true);
        assertThat(warnings()).isEmpty();
    }

    @Test
    void should_take_a_conflict_without_the_code_for_a_gate_that_did_not_open() {
        server.enqueue(error(409, "SOMETHING_ELSE"));

        expectOpening(false);
        assertThat(warnings()).containsExactly("The gate not opened for the entrance panel: gate-service answered 409");
    }

    @ParameterizedTest
    @ValueSource(ints = {400, 404, 500, 502, 503, 504})
    void should_take_any_other_error_for_a_gate_that_did_not_open_and_log_the_status_alone(final int status) {
        server.enqueue(error(status, "DEVICE_FAILED"));

        expectOpening(false);
        assertThat(warnings()).containsExactly("The gate not opened for the entrance panel: gate-service answered " + status);
        assertThat(log.list).noneMatch(event -> event.getFormattedMessage().contains("secret details"));
    }

    @ParameterizedTest
    @ValueSource(ints = {301, 302, 307})
    void should_take_a_redirect_for_a_gate_that_did_not_open(final int status) {
        server.enqueue(new MockResponse.Builder().code(status).addHeader(HttpHeaders.LOCATION, "/elsewhere").build());

        expectOpening(false);
        assertThat(warnings()).containsExactly("The gate not opened for the entrance panel: gate-service answered " + status);
    }

    @SneakyThrows
    @Test
    void should_never_repeat_an_opening() {
        server.enqueue(error(503, "GATE_UNAVAILABLE"));
        server.enqueue(new MockResponse.Builder().code(204).build());

        expectOpening(false);

        assertThat(server.getRequestCount()).isEqualTo(1);
    }

    @SneakyThrows
    @Test
    void should_take_gate_service_away_for_a_gate_that_did_not_open() {
        server.close();

        expectOpening(false);
        assertThat(warnings()).singleElement()
            .satisfies(line -> assertThat(line).startsWith("The gate not opened for the entrance panel: "));
    }

    @Test
    void should_give_a_silent_gate_service_up_after_the_response_timeout() {
        sut = client(Duration.ofSeconds(1));
        server.enqueue(new MockResponse.Builder().headersDelay(5, TimeUnit.SECONDS).code(204).build());

        expectOpening(false);
        assertThat(warnings()).containsExactly(
            "No answer of gate-service within PT1S - the gate may have opened all the same; the entrance panel is told it did not");
    }

    private void expectOpening(final boolean opening) {
        sut.open()
            .as(StepVerifier::create)
            .expectNext(opening)
            .expectComplete()
            .verify(PATIENCE);
    }

    private List<String> warnings() {
        return log.list.stream()
            .filter(event -> event.getLevel() == Level.WARN)
            .map(ILoggingEvent::getFormattedMessage)
            .toList();
    }

    private static MockResponse error(final int status, final String code) {
        return new MockResponse.Builder()
            .code(status)
            .addHeader(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE)
            .body("""
                {"errors":[{"message":"Refused","details":"secret details","code":"%s"}]}
                """.formatted(code))
            .build();
    }

    //no pool: every test has a server of its own (see DeviceDatabaseClientTest, HAS-216)
    private GateClient client(final Duration responseTimeout) {
        return new GateClient(
            WebClient.builder().clientConnector(new ReactorClientHttpConnector(HttpClient.newConnection())).build(),
            new GateServiceProperties("localhost", server.getPort(), responseTimeout)
        );
    }
}

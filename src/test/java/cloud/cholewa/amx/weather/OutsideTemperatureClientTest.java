package cloud.cholewa.amx.weather;

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
import org.junit.jupiter.params.provider.CsvSource;
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

//The outside temperature of the entrance panel (HAS-486): what weather-service answers against what
//the panel is told - a failure is never a zero, and the level of the log line is what the alerts see
class OutsideTemperatureClientTest {

    private static final Duration PATIENCE = Duration.ofSeconds(10);

    private final ListAppender<ILoggingEvent> log = new ListAppender<>();
    private Level levelBefore;
    private MockWebServer server;
    private OutsideTemperatureClient sut;

    @SneakyThrows
    @BeforeEach
    void setUp() {
        server = new MockWebServer();
        server.start();
        sut = client(Duration.ofSeconds(5));
        final Logger logger = (Logger) LoggerFactory.getLogger(OutsideTemperatureClient.class);
        levelBefore = logger.getLevel();
        logger.setLevel(Level.DEBUG);
        log.start();
        logger.addAppender(log);
    }

    @SneakyThrows
    @AfterEach
    void tearDown() {
        final Logger logger = (Logger) LoggerFactory.getLogger(OutsideTemperatureClient.class);
        logger.detachAppender(log);
        logger.setLevel(levelBefore);
        server.close();
    }

    @ParameterizedTest
    @CsvSource({"3.4, 3.4", "-3.44, -3.4", "-3.45, -3.5", "21, 21.0", "0.04, 0.0", "-0.04, 0.0", "12.25, 12.3"})
    void should_show_the_temperature_with_one_decimal_and_a_point(final String value, final String shown) {
        answer("""
            {"humidity":{"value":81,"measuredAt":"2026-11-23T12:00:00","stale":false},
             "temperature":{"value":%s,"measuredAt":"2026-11-23T12:00:00","stale":false}}
            """.formatted(value));

        expectRead(shown);
    }

    //what weather-service 0.8.0 answered on 2026-11-23: raining is a boolean, the others numbers -
    //the first release of this client failed the whole answer on it
    @Test
    void should_read_the_temperature_from_the_whole_answer_whatever_the_other_metrics_hold() {
        answer("""
            {"battery":{"value":97,"measuredAt":"2026-11-23T14:09:34","stale":false},
             "pressure":{"value":980.2,"measuredAt":"2026-11-23T14:12:12","stale":false},
             "raining":{"value":false,"measuredAt":"2026-11-23T14:09:43","stale":false},
             "humidity":{"value":87,"measuredAt":"2026-11-23T14:10:29","stale":false},
             "temperature":{"value":0.8,"measuredAt":"2026-11-23T14:09:34","stale":false},
             "windDirection":{"value":258.0,"measuredAt":"2026-11-23T14:12:29","stale":false},
             "precipitation":{"value":5065,"measuredAt":"2026-11-23T14:09:34","stale":false},
             "airQualityLevel":{"value":1,"measuredAt":"2026-11-23T14:10:39","stale":false}}
            """);

        expectRead("0.8");
        assertThat(warnings()).isEmpty();
    }

    @ParameterizedTest
    @ValueSource(strings = {"true", "{\"c\":1}", "[1]"})
    void should_take_a_temperature_that_is_no_number_for_a_failed_call(final String value) {
        answer("{\"temperature\":{\"value\":" + value + ",\"stale\":false}}");

        expectRead("");
        assertThat(warnings()).hasSize(1);
    }

    @SneakyThrows
    @Test
    void should_ask_the_current_weather_of_weather_service() {
        answer("""
            {"temperature":{"value":3.4,"measuredAt":"2026-11-23T12:00:00","stale":false}}
            """);

        expectRead("3.4");

        final RecordedRequest request = server.takeRequest(5, TimeUnit.SECONDS);
        assertThat(request).isNotNull();
        assertThat(request.getMethod()).isEqualTo("GET");
        assertThat(request.getUrl().encodedPath()).isEqualTo("/home/weather/current");
    }

    @Test
    void should_show_a_stale_reading_as_not_known() {
        answer("""
            {"temperature":{"value":3.4,"measuredAt":"2026-11-23T10:00:00","stale":true}}
            """);

        expectRead("");
        assertThat(log.list).noneMatch(event -> event.getLevel().isGreaterOrEqual(Level.WARN));
    }

    @ParameterizedTest
    @ValueSource(strings = {
        "{}",
        "{\"humidity\":{\"value\":81,\"stale\":false}}",
        "{\"temperature\":{}}",
        "{\"temperature\":{\"value\":3.4}}",
        "{\"temperature\":{\"stale\":false}}",
        "{\"temperature\":{\"value\":\"warm\",\"stale\":false}}",
        "[]",
        "not json"
    })
    void should_show_an_answer_that_is_not_the_model_as_not_known_never_a_zero(final String body) {
        answer(body);

        expectRead("");
        assertThat(warnings()).hasSize(1);
    }

    @Test
    void should_show_an_answer_without_a_body_as_not_known() {
        server.enqueue(new MockResponse.Builder().code(200).build());

        expectRead("");
        assertThat(warnings()).hasSize(1);
    }

    @ParameterizedTest
    @ValueSource(ints = {404, 500, 502, 503})
    void should_show_an_error_of_weather_service_as_not_known(final int status) {
        server.enqueue(new MockResponse.Builder().code(status).body("{\"errors\":[]}").build());

        expectRead("");
        assertThat(warnings()).singleElement()
            .satisfies(line -> assertThat(line).contains("weather-service answered " + status));
    }

    @SneakyThrows
    @Test
    void should_show_weather_service_away_as_not_known() {
        server.close();

        expectRead("");
        assertThat(warnings()).hasSize(1);
    }

    @Test
    void should_give_a_silent_weather_service_up_after_the_response_timeout() {
        sut = client(Duration.ofSeconds(1));
        server.enqueue(new MockResponse.Builder().headersDelay(5, TimeUnit.SECONDS).body("{}").build());

        expectRead("");
        assertThat(warnings()).singleElement()
            .satisfies(line -> assertThat(line).contains("TimeoutException"));
    }

    @Test
    void should_warn_once_while_the_reads_keep_failing_and_tell_when_they_work_again() {
        server.enqueue(new MockResponse.Builder().code(503).build());
        server.enqueue(new MockResponse.Builder().code(503).build());
        answer("""
            {"temperature":{"value":3.4,"stale":false}}
            """);

        expectRead("");
        expectRead("");
        expectRead("3.4");

        assertThat(warnings()).hasSize(1);
        assertThat(log.list).filteredOn(event -> event.getLevel() == Level.DEBUG).hasSize(1);
        assertThat(log.list).filteredOn(event -> event.getLevel() == Level.INFO)
            .singleElement()
            .satisfies(event -> assertThat(event.getFormattedMessage()).contains("read from weather-service again"));
    }

    private void expectRead(final String shown) {
        sut.read()
            .as(StepVerifier::create)
            .expectNext(shown)
            .expectComplete()
            .verify(PATIENCE);
    }

    private List<String> warnings() {
        return log.list.stream()
            .filter(event -> event.getLevel() == Level.WARN)
            .map(ILoggingEvent::getFormattedMessage)
            .toList();
    }

    private void answer(final String body) {
        server.enqueue(new MockResponse.Builder()
            .addHeader(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE)
            .body(body)
            .build());
    }

    //no pool: every test has a server of its own (see DeviceDatabaseClientTest, HAS-216)
    private OutsideTemperatureClient client(final Duration responseTimeout) {
        return new OutsideTemperatureClient(
            WebClient.builder().clientConnector(new ReactorClientHttpConnector(HttpClient.newConnection())).build(),
            new WeatherServiceProperties("localhost", server.getPort(), responseTimeout)
        );
    }
}

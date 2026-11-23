package cloud.cholewa.amx.panelserver;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import cloud.cholewa.amx.config.PanelServerProperties;
import cloud.cholewa.amx.lineserver.LineServerStub;
import cloud.cholewa.amx.weather.OutsideTemperatureClient;
import lombok.SneakyThrows;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.net.ServerSocket;
import java.time.Clock;
import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.Mockito.when;

//The outside temperature on the entrance panel (HAS-486): the real panel client against a stand-in of
//the panel server, the read of weather-service played by a mock
@ExtendWith(MockitoExtension.class)
class PanelTemperatureTest {

    private static final Duration PATIENCE = Duration.ofSeconds(20);
    private static final Duration SETTLE = Duration.ofMillis(300);

    @Mock
    private OutsideTemperatureClient outsideTemperature;

    private LineServerStub server;
    private PanelServerClient panel;
    private PanelTemperature sut;

    @SneakyThrows
    @BeforeEach
    void setUp() {
        server = new LineServerStub();
        panel = client(server.port());
        panel.start();
        await().atMost(PATIENCE).until(panel::isConnected);
        sut = new PanelTemperature(outsideTemperature, panel, SETTLE);
    }

    @SneakyThrows
    @AfterEach
    void tearDown() {
        sut.stop();
        panel.stop();
        server.close();
    }

    @Test
    void should_tell_the_panel_the_temperature_once_and_again_only_when_it_changed() {
        when(outsideTemperature.read()).thenReturn(Mono.just("3.4"), Mono.just("3.4"), Mono.just("-0.5"));

        pass();
        pass();
        pass();

        assertThat(server.lines()).containsExactly("temp;3.4", "temp;-0.5");
    }

    @Test
    void should_tell_the_panel_a_temperature_not_known_with_an_empty_value() {
        when(outsideTemperature.read()).thenReturn(Mono.just("3.4"), Mono.just(""), Mono.just(""));

        pass();
        pass();
        pass();

        assertThat(server.lines()).containsExactly("temp;3.4", "temp;");
    }

    @SneakyThrows
    @Test
    void should_tell_a_new_connection_the_last_temperature_at_once() {
        when(outsideTemperature.read()).thenReturn(Mono.just("3.4"));
        pass();

        server.hangUp();

        await().atMost(PATIENCE).until(() -> server.connections() == 2 && server.lines().size() == 2);
        assertThat(server.lines()).containsExactly("temp;3.4", "temp;3.4");
    }

    @SneakyThrows
    @Test
    void should_tell_a_new_connection_nothing_before_the_first_read() {
        server.hangUp();

        await().atMost(PATIENCE).until(() -> server.connections() == 2 && panel.isConnected());
        assertThat(server.lines()).isEmpty();
    }

    @SneakyThrows
    @Test
    void should_not_ask_a_panel_server_that_is_not_connected_and_tell_it_once_it_is() {
        final int port = freePort();
        sut.stop();
        panel.stop();
        panel = client(port);
        panel.start();
        sut = new PanelTemperature(outsideTemperature, panel, SETTLE);
        when(outsideTemperature.read()).thenReturn(Mono.just("3.4"));

        pass();

        try (LineServerStub late = new LineServerStub(port)) {
            await().atMost(PATIENCE).until(() -> late.lines().size() == 1);
            assertThat(late.lines()).containsExactly("temp;3.4");
        }
    }

    @SneakyThrows
    @Test
    void should_not_tell_a_connection_that_did_not_hold_for_the_settle() {
        sut.stop();
        sut = new PanelTemperature(outsideTemperature, panel, Duration.ofSeconds(2));
        when(outsideTemperature.read()).thenReturn(Mono.just("3.4"));
        pass();

        //the second connection ends before it held, the third one holds: told once, the second never
        server.hangUp();
        await().atMost(PATIENCE).until(() -> server.connections() == 2 && panel.isConnected());
        server.hangUp();

        await().atMost(PATIENCE).until(() -> server.lines().size() == 2);
        await().during(Duration.ofMillis(2500)).atMost(PATIENCE).until(() -> server.lines().size() == 2);
        assertThat(server.connections()).isEqualTo(3);
        assertThat(server.lines()).containsExactly("temp;3.4", "temp;3.4");
    }

    @Test
    void should_warn_once_while_the_panel_keeps_refusing_the_same_value() {
        final Logger logger = (Logger) LoggerFactory.getLogger(PanelTemperature.class);
        final ListAppender<ILoggingEvent> log = new ListAppender<>();
        log.start();
        logger.addAppender(log);
        server.answers(line -> "ERR unknown command\n");
        when(outsideTemperature.read()).thenReturn(Mono.just("3.4"), Mono.just("3.4"), Mono.just("3.5"));

        try {
            pass();
            pass();
            pass();
        } finally {
            logger.detachAppender(log);
        }

        assertThat(log.list).filteredOn(event -> event.getLevel() == Level.WARN)
            .extracting(ILoggingEvent::getFormattedMessage)
            .satisfiesExactly(
                first -> assertThat(first).contains("[3.4]"),
                changed -> assertThat(changed).contains("[3.5]"));
    }

    @Test
    void should_tell_the_panel_again_with_the_next_pass_what_it_refused() {
        server.answers(line -> "ERR unknown command\n");
        when(outsideTemperature.read()).thenReturn(Mono.just("3.4"));

        pass();
        server.answers(line -> "OK\n");
        pass();
        pass();

        assertThat(server.lines()).containsExactly("temp;3.4", "temp;3.4");
    }

    @Test
    void should_read_the_temperature_with_every_run_of_the_scheduled_method_not_once() {
        when(outsideTemperature.read()).thenReturn(Mono.just("3.4"), Mono.just("3.5"));
        final Mono<Void> refresh = sut.refresh();

        refresh.as(StepVerifier::create).expectComplete().verify(PATIENCE);
        refresh.as(StepVerifier::create).expectComplete().verify(PATIENCE);

        assertThat(server.lines()).containsExactly("temp;3.4", "temp;3.5");
    }

    private void pass() {
        sut.pass()
            .as(StepVerifier::create)
            .expectComplete()
            .verify(PATIENCE);
    }

    private static PanelServerClient client(final int port) {
        return new PanelServerClient(
            new PanelServerProperties("127.0.0.1", port, Duration.ofSeconds(2), Duration.ofSeconds(5)),
            Clock.systemDefaultZone(),
            Duration.ofMillis(100),
            Duration.ofMillis(500),
            PanelServerClient.READ_IDLE_LIMIT,
            PanelServerClient.KEEPALIVE_AFTER
        );
    }

    @SneakyThrows
    private static int freePort() {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }
}

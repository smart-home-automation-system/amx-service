package cloud.cholewa.amx.panelserver;

import cloud.cholewa.amx.config.PanelServerProperties;
import cloud.cholewa.amx.gate.GateClient;
import cloud.cholewa.amx.lineserver.LineServerStub;
import io.micrometer.observation.ObservationRegistry;
import lombok.SneakyThrows;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import reactor.core.publisher.Mono;

import java.time.Clock;
import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

//The gate button of the entrance panel (HAS-487): the real panel client against a stand-in of the
//panel server, gate-service played by a mock of its client
@ExtendWith(MockitoExtension.class)
class PanelGateButtonTest {

    private static final Duration PATIENCE = Duration.ofSeconds(20);

    @Mock
    private GateClient gateClient;

    private LineServerStub server;
    private PanelServerClient panel;
    private PanelGateButton sut;

    @SneakyThrows
    @BeforeEach
    void setUp() {
        server = new LineServerStub();
        panel = new PanelServerClient(
            new PanelServerProperties("127.0.0.1", server.port(), Duration.ofSeconds(2), Duration.ofSeconds(5)),
            Clock.systemDefaultZone(),
            Duration.ofMillis(100),
            Duration.ofMillis(500),
            PanelServerClient.READ_IDLE_LIMIT,
            PanelServerClient.KEEPALIVE_AFTER
        );
        sut = new PanelGateButton(panel, gateClient, ObservationRegistry.create());
        sut.subscribe();
        panel.start();
        await().atMost(PATIENCE).until(panel::isConnected);
    }

    @SneakyThrows
    @AfterEach
    void tearDown() {
        sut.stop();
        panel.stop();
        server.close();
    }

    @SneakyThrows
    @Test
    void should_open_the_gate_at_a_press_and_tell_the_panel_it_is_opening() {
        when(gateClient.open()).thenReturn(Mono.just(true));

        server.push("gate-open\n");

        await().atMost(PATIENCE).until(() -> server.lines().size() == 1);
        assertThat(server.lines()).containsExactly("gate;ok");
        verify(gateClient, times(1)).open();
    }

    @SneakyThrows
    @Test
    void should_tell_the_panel_a_gate_that_did_not_open() {
        when(gateClient.open()).thenReturn(Mono.just(false));

        server.push("gate-open\n");

        await().atMost(PATIENCE).until(() -> server.lines().size() == 1);
        assertThat(server.lines()).containsExactly("gate;failed");
    }

    @SneakyThrows
    @Test
    void should_drop_a_press_while_one_is_handled_never_queue_it() {
        when(gateClient.open()).thenReturn(Mono.delay(Duration.ofMillis(500)).thenReturn(true), Mono.just(false));

        server.push("gate-open\ngate-open\ngate-open\n");

        await().atMost(PATIENCE).until(() -> server.lines().size() == 1);
        await().during(Duration.ofMillis(700)).atMost(PATIENCE).until(() -> server.lines().size() == 1);
        assertThat(server.lines()).containsExactly("gate;ok");
        verify(gateClient, times(1)).open();

        //once the press is handled, the next one is
        server.push("gate-open\n");
        await().atMost(PATIENCE).until(() -> server.lines().size() == 2);
        assertThat(server.lines()).containsExactly("gate;ok", "gate;failed");
    }

    @SneakyThrows
    @Test
    void should_handle_the_next_press_after_one_that_failed_unexpectedly() {
        when(gateClient.open()).thenReturn(Mono.error(new IllegalStateException("boom")), Mono.just(true));

        server.push("gate-open\n");
        await().atMost(PATIENCE).untilAsserted(() -> verify(gateClient, times(1)).open());
        server.push("gate-open\n");

        await().atMost(PATIENCE).until(() -> server.lines().size() == 1);
        assertThat(server.lines()).containsExactly("gate;ok");
    }

    @SneakyThrows
    @Test
    void should_keep_handling_presses_when_the_panel_could_not_be_told() {
        server.answers(line -> "ERR unknown command\n");
        when(gateClient.open()).thenReturn(Mono.just(true));

        server.push("gate-open\n");
        await().atMost(PATIENCE).until(() -> server.lines().size() == 1);
        server.answers(line -> "OK\n");
        server.push("gate-open\n");

        await().atMost(PATIENCE).until(() -> server.lines().size() == 2);
        assertThat(server.lines()).containsExactly("gate;ok", "gate;ok");
    }

    @SneakyThrows
    @Test
    void should_ignore_a_line_that_is_no_press() {
        server.push("OK\n");

        await().during(Duration.ofMillis(500)).atMost(PATIENCE).until(() -> server.lines().isEmpty());
    }
}

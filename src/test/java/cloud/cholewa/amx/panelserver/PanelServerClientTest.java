package cloud.cholewa.amx.panelserver;

import cloud.cholewa.amx.config.PanelServerProperties;
import cloud.cholewa.amx.infrastructure.error.CustomError;
import cloud.cholewa.amx.infrastructure.error.PanelCommandException;
import cloud.cholewa.amx.lineserver.LineServerStub;
import lombok.SneakyThrows;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpStatus;
import reactor.test.StepVerifier;

import java.net.ServerSocket;
import java.time.Clock;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

//The client of the panel server of the Office master (HAS-484). What it shares with the frame
//server - one command at a time, a timeout closing the connection, the reconnect - is held by
//FrameServerClientTest; here what is its own: the ring, the line of the gate button, the codes of a
//failed command and the keepalive
class PanelServerClientTest {

    private static final Duration TIMEOUT = Duration.ofSeconds(5);
    private static final Duration PATIENCE = Duration.ofSeconds(20);
    private static final Duration RECONNECT_MIN = Duration.ofMillis(100);
    private static final Duration RECONNECT_MAX = Duration.ofMillis(500);

    private LineServerStub server;
    private PanelServerClient sut;

    @SneakyThrows
    @BeforeEach
    void setUp() {
        server = new LineServerStub();
        sut = connectedClient(TIMEOUT, PanelServerClient.READ_IDLE_LIMIT, PanelServerClient.KEEPALIVE_AFTER);
    }

    @SneakyThrows
    @AfterEach
    void tearDown() {
        sut.stop();
        server.close();
    }

    @Test
    void should_ring_the_panel_with_one_line() {
        sut.ring()
            .as(StepVerifier::create)
            .expectComplete()
            .verify(PATIENCE);

        assertThat(server.lines()).containsExactly("ring");
    }

    @Test
    void should_report_a_refused_ring_as_a_bad_gateway_with_the_reason_of_the_master() {
        server.answers(line -> "ERR unknown command\n");

        sut.ring()
            .as(StepVerifier::create)
            .expectErrorSatisfies(failure(
                HttpStatus.BAD_GATEWAY,
                CustomError.PANEL_REFUSED,
                "the AMX panel server refused the command: unknown command"
            ))
            .verify(PATIENCE);
    }

    @Test
    void should_report_an_unanswered_ring_as_a_gateway_timeout() {
        sut.stop();
        sut = connectedClient(Duration.ofMillis(300), PanelServerClient.READ_IDLE_LIMIT, PanelServerClient.KEEPALIVE_AFTER);
        server.answers(line -> null);

        sut.ring()
            .as(StepVerifier::create)
            .expectErrorSatisfies(failure(
                HttpStatus.GATEWAY_TIMEOUT,
                CustomError.PANEL_SERVER_TIMEOUT,
                "the AMX panel server did not answer within PT0.3S; the command may have been carried out"
            ))
            .verify(PATIENCE);
    }

    //the Office master switched off: the ring is refused at once, a 503 - and only the panel waits
    @SneakyThrows
    @Test
    void should_refuse_a_ring_while_the_panel_server_is_not_connected_as_unavailable() {
        sut.stop();
        final int port;
        try (ServerSocket free = new ServerSocket(0)) {
            port = free.getLocalPort();
        }
        sut = new PanelServerClient(
            new PanelServerProperties("127.0.0.1", port, Duration.ofSeconds(1), TIMEOUT),
            Clock.systemDefaultZone(), RECONNECT_MIN, RECONNECT_MAX,
            PanelServerClient.READ_IDLE_LIMIT, PanelServerClient.KEEPALIVE_AFTER
        );
        sut.start();

        sut.ring()
            .as(StepVerifier::create)
            .expectErrorSatisfies(failure(
                HttpStatus.SERVICE_UNAVAILABLE,
                CustomError.PANEL_SERVER_UNAVAILABLE,
                "the AMX panel server is not connected; the command was not sent"
            ))
            .verify(PATIENCE);
        assertThat(sut.isConnected()).isFalse();
    }

    @Test
    void should_refuse_a_ring_after_the_stop() {
        sut.stop();

        sut.ring()
            .as(StepVerifier::create)
            .expectErrorSatisfies(failure(
                HttpStatus.SERVICE_UNAVAILABLE,
                CustomError.PANEL_SERVER_UNAVAILABLE,
                "the panel server client is shut down; the command was not sent"
            ))
            .verify(PATIENCE);
        assertThat(server.lines()).isEmpty();
    }

    //the gate button: told the moment it comes, also between a command and its answer, never
    //taken for an answer
    @SneakyThrows
    @Test
    void should_hand_on_the_gate_button_even_between_a_command_and_its_answer() {
        final List<PanelLine> lines = new CopyOnWriteArrayList<>();
        sut.panelLines().subscribe(lines::add);
        server.answers(line -> "gate-open\r\nOK\n");

        sut.ring()
            .as(StepVerifier::create)
            .expectComplete()
            .verify(PATIENCE);

        await().atMost(PATIENCE).until(() -> lines.size() == 1);
        assertThat(lines).containsExactly(PanelLine.GATE_OPEN);
    }

    @SneakyThrows
    @Test
    void should_hand_on_the_gate_button_pressed_unasked() {
        final List<PanelLine> lines = new CopyOnWriteArrayList<>();
        sut.panelLines().subscribe(lines::add);

        server.push("gate-open\n");

        await().atMost(PATIENCE).until(() -> lines.size() == 1);
        assertThat(lines).containsExactly(PanelLine.GATE_OPEN);
    }

    //nobody listens before HAS-487: a press is logged and dropped, the connection stays
    @SneakyThrows
    @Test
    void should_drop_a_gate_button_nobody_listens_to_and_keep_the_connection() {
        server.push("gate-open\n");
        server.push("something new\n");

        sut.ring()
            .as(StepVerifier::create)
            .expectComplete()
            .verify(PATIENCE);
        assertThat(server.connections()).isEqualTo(1);
    }

    @Test
    void should_end_the_lines_at_the_stop() {
        final StepVerifier lines = sut.panelLines().as(StepVerifier::create).expectComplete().verifyLater();

        sut.stop();

        lines.verify(PATIENCE);
    }

    //the panel says nothing for hours: a quiet connection is asked ping, and its OK keeps it from
    //ever looking dead to the read idle limit
    @SneakyThrows
    @Test
    void should_ask_ping_on_a_quiet_connection_and_keep_it() {
        sut.stop();
        final int before = server.connections();
        sut = connectedClient(TIMEOUT, Duration.ofSeconds(3), Duration.ofMillis(200));

        await().atMost(PATIENCE).until(() -> server.lines().stream().filter("ping"::equals).count() >= 15);
        assertThat(server.connections()).isEqualTo(before + 1);
        assertThat(sut.isConnected()).isTrue();
    }

    //a dead connection leaves the ping unanswered: replaced within the response timeout, not after
    //the read idle limit
    @SneakyThrows
    @Test
    void should_replace_a_connection_that_leaves_the_ping_unanswered() {
        sut.stop();
        final int before = server.connections();
        server.answers(line -> "ping".equals(line) ? null : "OK\n");
        sut = connectedClient(Duration.ofMillis(300), PanelServerClient.READ_IDLE_LIMIT, Duration.ofMillis(200));

        await().atMost(PATIENCE).until(() -> server.connections() >= before + 2);
    }

    //a command between the ticks is a line from the server too: no ping while the panel talks
    @SneakyThrows
    @Test
    void should_not_ask_ping_while_lines_arrive() {
        sut.stop();
        sut = connectedClient(TIMEOUT, PanelServerClient.READ_IDLE_LIMIT, Duration.ofMillis(1500));

        for (int i = 0; i < 6; i++) {
            pause(Duration.ofMillis(200));
            server.push("gate-open\n");
        }

        assertThat(server.lines()).doesNotContain("ping");
    }

    private PanelServerClient connectedClient(
        final Duration responseTimeout,
        final Duration readIdleLimit,
        final Duration keepaliveAfter
    ) {
        final PanelServerClient client = new PanelServerClient(
            new PanelServerProperties("127.0.0.1", server.port(), Duration.ofSeconds(2), responseTimeout),
            Clock.systemDefaultZone(),
            RECONNECT_MIN,
            RECONNECT_MAX,
            readIdleLimit,
            keepaliveAfter
        );
        client.start();
        await().atMost(PATIENCE).until(client::isConnected);
        return client;
    }

    private static Consumer<Throwable> failure(final HttpStatus status, final CustomError error, final String details) {
        return throwable -> assertThat(throwable)
            .isInstanceOfSatisfying(PanelCommandException.class, exception -> {
                assertThat(exception.getHttpStatus()).isEqualTo(status);
                assertThat(exception.getError()).isEqualTo(error);
                assertThat(exception.getMessage()).isEqualTo(details);
            });
    }

    //the time a test waits for nothing to happen
    @SuppressWarnings("java:S2925")
    @SneakyThrows
    private static void pause(final Duration duration) {
        Thread.sleep(duration);
    }
}

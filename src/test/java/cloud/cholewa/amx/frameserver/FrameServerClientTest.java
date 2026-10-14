package cloud.cholewa.amx.frameserver;

import cloud.cholewa.amx.config.FrameServerProperties;
import cloud.cholewa.amx.infrastructure.error.CustomError;
import cloud.cholewa.amx.infrastructure.error.EatonCommandException;
import cloud.cholewa.home.model.EatonGatewayType;
import lombok.SneakyThrows;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.http.HttpStatus;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import java.net.ServerSocket;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

class FrameServerClientTest {

    private static final String OPEN = "5A,06,B1,05,0E,01,00,A5";
    private static final String STOP = "5A,06,B1,05,0E,02,00,A5";
    private static final String CLOSE = "5A,06,B1,05,0E,00,00,A5";

    private static final String TEMPERATURE = "5A,0C,C1,2D,62,17,00,00,F0,00,00,3A,10,A5";
    private static final String ACKNOWLEDGED = "5A,08,C3,1C,04,00,10,00,00,A5";

    //generous on purpose: the first connection of a JVM initialises Netty. The tests of a silent
    //server bring a short one of their own
    private static final Duration TIMEOUT = Duration.ofSeconds(5);
    private static final Duration PATIENCE = Duration.ofSeconds(20);
    //the client connects again quickly here; in the application it is a second, up to thirty
    private static final Duration RECONNECT_MIN = Duration.ofMillis(100);
    private static final Duration RECONNECT_MAX = Duration.ofMillis(500);

    private FrameServerStub server;
    private FrameServerClient sut;

    @SneakyThrows
    @BeforeEach
    void setUp() {
        server = new FrameServerStub();
        sut = connectedClient(TIMEOUT);
    }

    @SneakyThrows
    @AfterEach
    void tearDown() {
        sut.stop();
        server.close();
    }

    //the connection is the only way to receive, so it is not left to the first command
    @Test
    void should_connect_at_the_start_without_a_command() {
        assertThat(sut.isConnected()).isTrue();
        assertThat(server.connections()).isEqualTo(1);
        assertThat(server.lines()).isEmpty();
    }

    @Test
    void should_send_the_gateway_and_the_frame_as_one_line() {
        sut.send(EatonGatewayType.BLINDS, OPEN)
            .as(StepVerifier::create)
            .expectComplete()
            .verify(PATIENCE);

        assertThat(server.lines()).containsExactly("blinds;" + OPEN);
    }

    @Test
    void should_name_the_lights_gateway_by_its_value() {
        sut.send(EatonGatewayType.LIGHTS, "5A,06,B1,07,0A,01,00,A5")
            .as(StepVerifier::create)
            .expectComplete()
            .verify(PATIENCE);

        assertThat(server.lines()).containsExactly("lights;5A,06,B1,07,0A,01,00,A5");
    }

    @ParameterizedTest
    @ValueSource(strings = {"OK\n", "OK\r\n", "OK \n"})
    void should_take_ok_however_the_line_ends(final String answer) {
        server.answers(line -> answer);

        sut.send(EatonGatewayType.BLINDS, OPEN)
            .as(StepVerifier::create)
            .expectComplete()
            .verify(PATIENCE);
    }

    @Test
    void should_keep_one_connection_for_the_commands_that_follow() {
        Flux.just(OPEN, STOP, CLOSE)
            .concatMap(frame -> sut.send(EatonGatewayType.BLINDS, frame))
            .as(StepVerifier::create)
            .expectComplete()
            .verify(PATIENCE);

        assertThat(server.lines()).containsExactly("blinds;" + OPEN, "blinds;" + STOP, "blinds;" + CLOSE);
        assertThat(server.connections()).isEqualTo(1);
    }

    //the answers carry nothing that ties them to a command: a second line before the answer to the
    //first would leave nobody able to say which of the two was refused
    @Test
    void should_send_a_command_only_after_the_one_before_it_was_answered() {
        server.thinksFor(Duration.ofMillis(150));

        Flux.just(OPEN, STOP, CLOSE)
            .flatMap(frame -> sut.send(EatonGatewayType.BLINDS, frame))
            .as(StepVerifier::create)
            .expectComplete()
            .verify(PATIENCE);

        assertThat(server.lines()).containsExactly("blinds;" + OPEN, "blinds;" + STOP, "blinds;" + CLOSE);
        assertThat(server.wasSentAhead()).isFalse();
        assertThat(server.connections()).isEqualTo(1);
    }

    @Test
    void should_report_a_refused_frame_with_the_reason_of_the_server_and_keep_the_connection() {
        server.answers(line -> line.endsWith(OPEN) ? "ERR gateway offline\n" : "OK\n");

        sut.send(EatonGatewayType.BLINDS, OPEN)
            .as(StepVerifier::create)
            .expectErrorSatisfies(failure(
                HttpStatus.BAD_GATEWAY,
                CustomError.FRAME_REFUSED,
                "the AMX frame server refused the frame: gateway offline"
            ))
            .verify(PATIENCE);

        sut.send(EatonGatewayType.BLINDS, STOP)
            .as(StepVerifier::create)
            .expectComplete()
            .verify(PATIENCE);

        assertThat(server.connections()).isEqualTo(1);
    }

    @Test
    void should_report_a_refusal_without_a_reason() {
        server.answers(line -> "ERR\n");

        sut.send(EatonGatewayType.BLINDS, OPEN)
            .as(StepVerifier::create)
            .expectErrorSatisfies(failure(
                HttpStatus.BAD_GATEWAY,
                CustomError.FRAME_REFUSED,
                "the AMX frame server refused the frame: no reason given"
            ))
            .verify(PATIENCE);
    }

    //the reason is answered to a caller behind the gateway
    @Test
    void should_keep_the_reason_of_a_refusal_short_and_printable() {
        server.answers(line -> "ERR \u0007bell " + "x".repeat(200) + "\n");

        sut.send(EatonGatewayType.BLINDS, OPEN)
            .as(StepVerifier::create)
            .expectErrorSatisfies(throwable -> assertThat(throwable.getMessage())
                .startsWith("the AMX frame server refused the frame: ?bell xxx")
                .hasSize("the AMX frame server refused the frame: ".length() + 100))
            .verify(PATIENCE);
    }

    @Test
    void should_answer_a_gateway_timeout_when_the_server_stays_silent() {
        sut.stop();
        sut = connectedClient(Duration.ofMillis(300));
        server.answers(line -> null);

        sut.send(EatonGatewayType.BLINDS, OPEN)
            .as(StepVerifier::create)
            .expectErrorSatisfies(failure(
                HttpStatus.GATEWAY_TIMEOUT,
                CustomError.FRAME_SERVER_TIMEOUT,
                "the AMX frame server did not answer within PT0.3S; the frame may have been carried out"
            ))
            .verify(PATIENCE);
    }

    //the answer to a command that was given up arrives while the next one waits for its own: on the
    //same connection the refusal of the first would be told to the caller of the second. The
    //connection is dropped, and the loop opens another
    @Test
    void should_not_take_a_late_answer_for_the_answer_of_the_next_command() {
        sut.stop();
        sut = connectedClient(Duration.ofMillis(300));
        //the client of setUp and this one
        final int before = server.connections();
        server.answers(line -> {
            if (line.endsWith(OPEN)) {
                pause(Duration.ofMillis(600));
                return "ERR late\n";
            }
            pause(Duration.ofMillis(100));
            return "OK\n";
        });

        sut.send(EatonGatewayType.BLINDS, OPEN)
            .as(StepVerifier::create)
            .expectErrorSatisfies(throwable -> assertThat(throwable)
                .isInstanceOfSatisfying(EatonCommandException.class, exception ->
                    assertThat(exception.getError()).isEqualTo(CustomError.FRAME_SERVER_TIMEOUT)))
            .verify(PATIENCE);

        await().atMost(PATIENCE).until(() -> server.connections() == before + 1 && sut.isConnected());

        sut.send(EatonGatewayType.BLINDS, STOP)
            .as(StepVerifier::create)
            .expectComplete()
            .verify(PATIENCE);

        assertThat(server.connections()).isEqualTo(before + 1);
    }

    //a frame of a gateway may arrive at any moment, also between a command and its answer
    @Test
    void should_not_take_a_frame_line_for_the_answer_of_a_command() {
        final List<ReceivedFrame> frames = new CopyOnWriteArrayList<>();
        sut.frames().subscribe(frames::add);
        server.answers(line -> "lights;" + ACKNOWLEDGED + "\nblinds;" + TEMPERATURE + "\nOK\n");

        sut.send(EatonGatewayType.LIGHTS, "5A,06,B1,07,0A,01,00,A5")
            .as(StepVerifier::create)
            .expectComplete()
            .verify(PATIENCE);

        await().atMost(PATIENCE).until(() -> frames.size() == 2);
        assertThat(frames).containsExactly(
            new ReceivedFrame(EatonGatewayType.LIGHTS, ACKNOWLEDGED),
            new ReceivedFrame(EatonGatewayType.BLINDS, TEMPERATURE)
        );
        assertThat(server.connections()).isEqualTo(1);
    }

    @SneakyThrows
    @Test
    void should_hand_over_the_frames_of_the_server_in_the_order_they_came() {
        final List<ReceivedFrame> frames = new CopyOnWriteArrayList<>();
        sut.frames().subscribe(frames::add);

        server.push("blinds;" + TEMPERATURE + "\n");
        server.push("lights;" + ACKNOWLEDGED + "\r\n");
        server.push("blinds;5A,0C,C1,2E,62,17,00,00,E2,00,00,41,10,A5\n");

        await().atMost(PATIENCE).until(() -> frames.size() == 3);
        assertThat(frames).containsExactly(
            new ReceivedFrame(EatonGatewayType.BLINDS, TEMPERATURE),
            new ReceivedFrame(EatonGatewayType.LIGHTS, ACKNOWLEDGED),
            new ReceivedFrame(EatonGatewayType.BLINDS, "5A,0C,C1,2E,62,17,00,00,E2,00,00,41,10,A5")
        );
    }

    //the frames come whether a command was ever sent or not, as long as the client runs
    @SneakyThrows
    @Test
    void should_receive_frames_on_a_connection_nothing_was_sent_on() {
        final List<ReceivedFrame> frames = new CopyOnWriteArrayList<>();
        sut.frames().subscribe(frames::add);
        pause(Duration.ofMillis(500));

        server.push("blinds;" + TEMPERATURE + "\n");

        await().atMost(PATIENCE).until(() -> frames.size() == 1);
        assertThat(server.connections()).isEqualTo(1);
        assertThat(sut.isConnected()).isTrue();
    }

    //an empty line, an answer nobody waits for and a line that is neither a frame nor an answer:
    //logged and ignored, the connection kept, the next command and the next frame served
    @SneakyThrows
    @ParameterizedTest
    @ValueSource(strings = {"\n", "\r\n", "OK\n", "ERR nobody asked\n", "HTTP/1.1 400 Bad Request\n", "ERROR\n", "kitchen;5A,A5\n"})
    void should_ignore_a_line_that_is_not_for_anyone(final String line) {
        final List<ReceivedFrame> frames = new CopyOnWriteArrayList<>();
        sut.frames().subscribe(frames::add);

        server.push(line);
        pause(Duration.ofMillis(200));

        sut.send(EatonGatewayType.BLINDS, OPEN)
            .as(StepVerifier::create)
            .expectComplete()
            .verify(PATIENCE);
        server.push("blinds;" + TEMPERATURE + "\n");

        await().atMost(PATIENCE).until(() -> frames.size() == 1);
        assertThat(frames).containsExactly(new ReceivedFrame(EatonGatewayType.BLINDS, TEMPERATURE));
        assertThat(server.connections()).isEqualTo(1);
    }

    //a line that is not an answer is never taken for one, whatever it looks like: the command waits
    //for its answer and, without one, times out
    @Test
    void should_not_read_a_word_that_starts_with_err_as_a_refusal() {
        sut.stop();
        sut = connectedClient(Duration.ofMillis(300));
        server.answers(line -> "ERROR\n");

        sut.send(EatonGatewayType.BLINDS, OPEN)
            .as(StepVerifier::create)
            .expectErrorSatisfies(throwable -> assertThat(throwable)
                .isInstanceOfSatisfying(EatonCommandException.class, exception ->
                    assertThat(exception.getError()).isEqualTo(CustomError.FRAME_SERVER_TIMEOUT)))
            .verify(PATIENCE);
    }

    @Test
    void should_answer_a_bad_gateway_when_the_server_hangs_up_instead_of_answering() {
        server.answers(line -> FrameServerStub.HANG_UP);

        sut.send(EatonGatewayType.BLINDS, OPEN)
            .as(StepVerifier::create)
            .expectErrorSatisfies(failure(
                HttpStatus.BAD_GATEWAY,
                CustomError.FRAME_SERVER_UNAVAILABLE,
                "the connection to the AMX frame server was lost before its answer;"
                    + " the frame may have been carried out"
            ))
            .verify(PATIENCE);
    }

    //a master that is switched off: every command is told at once, the connecting goes on behind
    @SneakyThrows
    @Test
    void should_refuse_commands_at_once_while_not_connected() {
        final int nobody;
        try (ServerSocket taken = new ServerSocket(0)) {
            nobody = taken.getLocalPort();
        }
        sut.stop();
        sut = new FrameServerClient(
            new FrameServerProperties("127.0.0.1", nobody, Duration.ofSeconds(2), TIMEOUT), RECONNECT_MIN, RECONNECT_MAX);
        sut.start();

        final long started = System.nanoTime();
        Flux.range(0, 6)
            .flatMap(each -> sut.send(EatonGatewayType.BLINDS, STOP).materialize())
            .as(StepVerifier::create)
            .thenConsumeWhile(signal -> signal.getThrowable() instanceof EatonCommandException exception
                && exception.getHttpStatus() == HttpStatus.BAD_GATEWAY
                && exception.getError() == CustomError.FRAME_SERVER_UNAVAILABLE
                && exception.getMessage().equals("the AMX frame server is not connected; the frame was not sent"))
            .expectComplete()
            .verify(PATIENCE);

        assertThat(System.nanoTime() - started).isLessThan(Duration.ofSeconds(1).toNanos());
        assertThat(sut.isConnected()).isFalse();
    }

    @SneakyThrows
    @Test
    void should_connect_again_after_the_server_closed_the_connection() {
        final List<ReceivedFrame> frames = new CopyOnWriteArrayList<>();
        sut.frames().subscribe(frames::add);

        server.hangUp();
        await().atMost(PATIENCE).until(() -> server.connections() == 2 && sut.isConnected());

        sut.send(EatonGatewayType.BLINDS, STOP)
            .as(StepVerifier::create)
            .expectComplete()
            .verify(PATIENCE);
        server.push("blinds;" + TEMPERATURE + "\n");

        await().atMost(PATIENCE).until(() -> frames.size() == 1);
        assertThat(server.lines()).containsExactly("blinds;" + STOP);
    }

    //the master is restarted, or the pod comes up before it: the attempts go on until one succeeds
    @SneakyThrows
    @Test
    void should_keep_trying_until_the_server_is_there() {
        final int port;
        try (ServerSocket taken = new ServerSocket(0)) {
            port = taken.getLocalPort();
        }
        sut.stop();
        sut = new FrameServerClient(
            new FrameServerProperties("127.0.0.1", port, Duration.ofSeconds(2), TIMEOUT), RECONNECT_MIN, RECONNECT_MAX);
        sut.start();
        pause(Duration.ofMillis(700));
        assertThat(sut.isConnected()).isFalse();

        try (FrameServerStub late = new FrameServerStub(port)) {
            await().atMost(PATIENCE).until(sut::isConnected);

            sut.send(EatonGatewayType.BLINDS, OPEN)
                .as(StepVerifier::create)
                .expectComplete()
                .verify(PATIENCE);

            assertThat(late.lines()).containsExactly("blinds;" + OPEN);
            assertThat(late.connections()).isEqualTo(1);
        }
    }

    //the pod is stopped in a rollout: nobody is left waiting for the gateway to give up, and only
    //the command whose line was written is told that it may have been carried out
    @Test
    void should_tell_the_commands_that_wait_at_the_stop_what_became_of_them() {
        server.answers(line -> null);
        final AtomicReference<Throwable> written = new AtomicReference<>();
        final AtomicReference<Throwable> queued = new AtomicReference<>();

        sut.send(EatonGatewayType.BLINDS, OPEN).subscribe(nothing -> { }, written::set);
        await().atMost(PATIENCE).until(() -> server.lines().size() == 1);
        sut.send(EatonGatewayType.BLINDS, STOP).subscribe(nothing -> { }, queued::set);

        sut.stop();

        await().atMost(PATIENCE).until(() -> written.get() != null && queued.get() != null);
        assertThat(written.get()).hasMessage(
            "the connection to the AMX frame server was lost before its answer; the frame may have been carried out");
        assertThat(queued.get()).hasMessage("the frame server client is shut down; the frame was not sent");
        assertThat(server.lines()).containsExactly("blinds;" + OPEN);
    }

    //the receiver ends with the client, not with an error - and the master is not connected to again
    @Test
    void should_end_the_frames_and_the_connection_at_the_stop() {
        final StepVerifier frames = sut.frames().as(StepVerifier::create).expectComplete().verifyLater();

        sut.stop();

        frames.verify(PATIENCE);
        assertThat(sut.isConnected()).isFalse();
        assertThat(sut.isRunning()).isFalse();
        pause(Duration.ofMillis(400));
        assertThat(server.connections()).isEqualTo(1);
    }

    //a caller that gave up - the gateway after its own timeout - must not move a blind later
    @Test
    void should_not_send_a_command_its_caller_gave_up_before_it_was_started() {
        server.thinksFor(Duration.ofMillis(400));

        sut.send(EatonGatewayType.BLINDS, OPEN).subscribe();
        await().atMost(PATIENCE).until(() -> server.lines().size() == 1);

        final Disposable givenUp = sut.send(EatonGatewayType.BLINDS, CLOSE).subscribe();
        givenUp.dispose();

        sut.send(EatonGatewayType.BLINDS, STOP)
            .as(StepVerifier::create)
            .expectComplete()
            .verify(PATIENCE);

        assertThat(server.lines()).containsExactly("blinds;" + OPEN, "blinds;" + STOP);
    }

    @Test
    void should_refuse_a_command_after_the_stop() {
        sut.stop();

        sut.send(EatonGatewayType.BLINDS, OPEN)
            .as(StepVerifier::create)
            .expectErrorSatisfies(throwable -> assertThat(throwable)
                .isInstanceOfSatisfying(EatonCommandException.class, exception ->
                    assertThat(exception.getError()).isEqualTo(CustomError.FRAME_SERVER_UNAVAILABLE)))
            .verify(PATIENCE);

        assertThat(server.lines()).isEmpty();
    }

    private FrameServerClient connectedClient(final Duration responseTimeout) {
        final FrameServerClient client = new FrameServerClient(
            new FrameServerProperties("127.0.0.1", server.port(), Duration.ofSeconds(2), responseTimeout),
            RECONNECT_MIN,
            RECONNECT_MAX
        );
        client.start();
        await().atMost(PATIENCE).until(client::isConnected);
        return client;
    }

    private static Consumer<Throwable> failure(final HttpStatus status, final CustomError error, final String details) {
        return throwable -> assertThat(throwable)
            .isInstanceOfSatisfying(EatonCommandException.class, exception -> {
                assertThat(exception.getHttpStatus()).isEqualTo(status);
                assertThat(exception.getError()).isEqualTo(error);
                assertThat(exception.getMessage()).isEqualTo(details);
            });
    }

    //the time the stub of the server takes before it answers, or a test waits for nothing to happen
    @SuppressWarnings("java:S2925")
    @SneakyThrows
    private static void pause(final Duration duration) {
        Thread.sleep(duration);
    }
}

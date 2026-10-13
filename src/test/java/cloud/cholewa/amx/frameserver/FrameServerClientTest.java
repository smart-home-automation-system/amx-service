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
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

class FrameServerClientTest {

    private static final String OPEN = "5A,09,B1,05,0E,01,00,00,00,00,A5";
    private static final String STOP = "5A,09,B1,05,0E,02,00,00,00,00,A5";
    private static final String CLOSE = "5A,09,B1,05,0E,00,00,00,00,00,A5";

    //generous on purpose: the first connection of a JVM initialises Netty. The tests of a silent
    //server bring a short one of their own
    private static final Duration TIMEOUT = Duration.ofSeconds(5);
    private static final Duration PATIENCE = Duration.ofSeconds(20);

    private FrameServerStub server;
    private FrameServerClient sut;

    @SneakyThrows
    @BeforeEach
    void setUp() {
        server = new FrameServerStub();
        sut = client(TIMEOUT, FrameServerClient.IDLE_LIMIT);
    }

    @SneakyThrows
    @AfterEach
    void tearDown() {
        sut.shutDown();
        server.close();
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
        sut.send(EatonGatewayType.LIGHTS, "5A,09,B1,07,0A,01,00,00,00,00,A5")
            .as(StepVerifier::create)
            .expectComplete()
            .verify(PATIENCE);

        assertThat(server.lines()).containsExactly("lights;5A,09,B1,07,0A,01,00,00,00,00,A5");
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
        server.answers(line -> line.endsWith(OPEN) ? "ERR serial not ready\n" : "OK\n");

        sut.send(EatonGatewayType.BLINDS, OPEN)
            .as(StepVerifier::create)
            .expectErrorSatisfies(failure(
                HttpStatus.BAD_GATEWAY,
                CustomError.FRAME_REFUSED,
                "the AMX frame server refused the frame: serial not ready"
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
        sut.shutDown();
        sut = client(Duration.ofMillis(300), FrameServerClient.IDLE_LIMIT);
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
    //same connection the refusal of the first would be told to the caller of the second
    @Test
    void should_not_take_a_late_answer_for_the_answer_of_the_next_command() {
        sut.shutDown();
        sut = client(Duration.ofMillis(300), FrameServerClient.IDLE_LIMIT);
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

        sut.send(EatonGatewayType.BLINDS, STOP)
            .as(StepVerifier::create)
            .expectComplete()
            .verify(PATIENCE);

        assertThat(server.connections()).isEqualTo(2);
    }

    @Test
    void should_drop_the_connection_after_an_answer_that_is_neither_ok_nor_err() {
        server.answers(line -> line.endsWith(OPEN) ? "HTTP/1.1 400 Bad Request\n" : "OK\n");

        sut.send(EatonGatewayType.BLINDS, OPEN)
            .as(StepVerifier::create)
            .expectErrorSatisfies(failure(
                HttpStatus.BAD_GATEWAY,
                CustomError.FRAME_SERVER_UNAVAILABLE,
                "the AMX frame server gave an answer that is neither OK nor ERR;"
                    + " the frame may have been carried out"
            ))
            .verify(PATIENCE);

        sut.send(EatonGatewayType.BLINDS, STOP)
            .as(StepVerifier::create)
            .expectComplete()
            .verify(PATIENCE);

        assertThat(server.connections()).isEqualTo(2);
    }

    //ERROR is not ERR: a word that only starts like a refusal is not read as one
    @Test
    void should_not_read_a_word_that_starts_with_err_as_a_refusal() {
        server.answers(line -> "ERROR\n");

        sut.send(EatonGatewayType.BLINDS, OPEN)
            .as(StepVerifier::create)
            .expectErrorSatisfies(throwable -> assertThat(throwable)
                .isInstanceOfSatisfying(EatonCommandException.class, exception ->
                    assertThat(exception.getError()).isEqualTo(CustomError.FRAME_SERVER_UNAVAILABLE)))
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

    @SneakyThrows
    @Test
    void should_answer_a_bad_gateway_when_nothing_listens() {
        final int nobody;
        try (ServerSocket taken = new ServerSocket(0)) {
            nobody = taken.getLocalPort();
        }
        sut.shutDown();
        sut = new FrameServerClient(
            new FrameServerProperties("127.0.0.1", nobody, Duration.ofSeconds(2), TIMEOUT));

        sut.send(EatonGatewayType.BLINDS, OPEN)
            .as(StepVerifier::create)
            .expectErrorSatisfies(failure(
                HttpStatus.BAD_GATEWAY,
                CustomError.FRAME_SERVER_UNAVAILABLE,
                "the AMX frame server cannot be reached; the frame was not sent"
            ))
            .verify(PATIENCE);
    }

    @SneakyThrows
    @Test
    void should_connect_again_after_the_server_closed_the_connection() {
        sut.send(EatonGatewayType.BLINDS, OPEN)
            .as(StepVerifier::create)
            .expectComplete()
            .verify(PATIENCE);

        server.hangUp();
        await().atMost(PATIENCE).until(() -> !sut.hasOpenConnection());

        sut.send(EatonGatewayType.BLINDS, STOP)
            .as(StepVerifier::create)
            .expectComplete()
            .verify(PATIENCE);

        assertThat(server.connections()).isEqualTo(2);
    }

    @Test
    void should_close_a_connection_nothing_was_sent_on_for_the_idle_limit_and_open_it_again() {
        sut.shutDown();
        sut = client(TIMEOUT, Duration.ofMillis(200));

        sut.send(EatonGatewayType.BLINDS, OPEN)
            .as(StepVerifier::create)
            .expectComplete()
            .verify(PATIENCE);

        await().atMost(PATIENCE).until(() -> !sut.hasOpenConnection());

        sut.send(EatonGatewayType.BLINDS, STOP)
            .as(StepVerifier::create)
            .expectComplete()
            .verify(PATIENCE);

        assertThat(server.lines()).containsExactly("blinds;" + OPEN, "blinds;" + STOP);
        assertThat(server.connections()).isEqualTo(2);
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
    void should_refuse_a_command_after_the_shutdown() {
        sut.shutDown();

        sut.send(EatonGatewayType.BLINDS, OPEN)
            .as(StepVerifier::create)
            .expectErrorSatisfies(throwable -> assertThat(throwable)
                .isInstanceOfSatisfying(EatonCommandException.class, exception ->
                    assertThat(exception.getError()).isEqualTo(CustomError.FRAME_SERVER_UNAVAILABLE)))
            .verify(PATIENCE);

        assertThat(server.lines()).isEmpty();
    }

    private FrameServerClient client(final Duration responseTimeout, final Duration idleLimit) {
        return new FrameServerClient(
            new FrameServerProperties("127.0.0.1", server.port(), Duration.ofSeconds(2), responseTimeout),
            idleLimit
        );
    }

    private static Consumer<Throwable> failure(final HttpStatus status, final CustomError error, final String details) {
        return throwable -> assertThat(throwable)
            .isInstanceOfSatisfying(EatonCommandException.class, exception -> {
                assertThat(exception.getHttpStatus()).isEqualTo(status);
                assertThat(exception.getError()).isEqualTo(error);
                assertThat(exception.getMessage()).isEqualTo(details);
            });
    }

    @SneakyThrows
    private static void pause(final Duration duration) {
        Thread.sleep(duration);
    }
}

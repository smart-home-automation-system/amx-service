package cloud.cholewa.amx.api;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import cloud.cholewa.amx.config.ExceptionHandlerConfig;
import cloud.cholewa.amx.eaton.frame.BlindsCommand;
import cloud.cholewa.amx.infrastructure.error.ConfigurationCallException;
import cloud.cholewa.amx.infrastructure.error.CustomError;
import cloud.cholewa.amx.infrastructure.error.EatonCommandException;
import cloud.cholewa.amx.infrastructure.error.processor.EatonCommandExceptionProcessor;
import cloud.cholewa.amx.service.EatonCommandService;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.webflux.test.autoconfigure.WebFluxTest;
import org.springframework.context.annotation.Import;
import org.springframework.http.HttpStatus;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.reactive.server.WebTestClient;
import reactor.core.publisher.Mono;

import java.time.LocalDateTime;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

//what a caller of the commands gets: the status, the body and - for a failure - the code and the
//level of the log line, which is what the alerts look at
@Import(ExceptionHandlerConfig.class)
@WebFluxTest(EatonCommandController.class)
class EatonCommandControllerTest {

    private static final LocalDateTime SENT_AT = LocalDateTime.of(2026, 10, 13, 18, 30, 5);

    private final ListAppender<ILoggingEvent> processorLog = new ListAppender<>();

    @Autowired
    private WebTestClient webTestClient;

    @MockitoBean
    private EatonCommandService eatonCommandService;

    @BeforeEach
    void setUp() {
        processorLog.start();
        ((Logger) LoggerFactory.getLogger(EatonCommandExceptionProcessor.class)).addAppender(processorLog);
    }

    @AfterEach
    void tearDown() {
        ((Logger) LoggerFactory.getLogger(EatonCommandExceptionProcessor.class)).detachAppender(processorLog);
    }

    @ParameterizedTest
    @CsvSource({"open,OPEN", "close,CLOSE", "stop,STOP"})
    void should_accept_a_blinds_command_and_say_when_the_frame_was_sent(
        final String parameter,
        final BlindsCommand command
    ) {
        when(eatonCommandService.moveBlinds(5, command)).thenReturn(Mono.just(SENT_AT));

        webTestClient.post()
            .uri("/amx/eaton/blinds/5?command={command}", parameter)
            .exchange()
            .expectStatus().isAccepted()
            .expectBody()
            .jsonPath("$.sentAt").isEqualTo("2026-10-13T18:30:05");
    }

    //HAS-291: a path of its own, which takes no command - a command sent along changes nothing
    @ParameterizedTest
    @ValueSource(strings = {"/amx/eaton/blinds/9/status", "/amx/eaton/blinds/9/status?command=open"})
    void should_accept_a_blinds_status_request_and_say_when_the_frame_was_sent(final String uri) {
        when(eatonCommandService.requestBlindsStatus(9)).thenReturn(Mono.just(SENT_AT));

        webTestClient.post()
            .uri(uri)
            .exchange()
            .expectStatus().isAccepted()
            .expectBody()
            .jsonPath("$.sentAt").isEqualTo("2026-10-13T18:30:05");

        verify(eatonCommandService).requestBlindsStatus(9);
        verifyNoMoreInteractions(eatonCommandService);
    }

    @ParameterizedTest
    @CsvSource({"on,true", "off,false"})
    void should_accept_a_light_switch(final String turn, final boolean on) {
        when(eatonCommandService.switchLight(7, on)).thenReturn(Mono.just(SENT_AT));

        webTestClient.post()
            .uri("/amx/eaton/lights/7?turn={turn}", turn)
            .exchange()
            .expectStatus().isAccepted()
            .expectBody()
            .jsonPath("$.sentAt").isEqualTo("2026-10-13T18:30:05");
    }

    @ParameterizedTest
    @ValueSource(ints = {0, 50, 100})
    void should_accept_a_light_level(final int value) {
        when(eatonCommandService.setLightLevel(12, value)).thenReturn(Mono.just(SENT_AT));

        webTestClient.post()
            .uri("/amx/eaton/lights/12/level?value={value}", value)
            .exchange()
            .expectStatus().isAccepted()
            .expectBody()
            .jsonPath("$.sentAt").isEqualTo("2026-10-13T18:30:05");
    }

    @ParameterizedTest
    @ValueSource(strings = {
        "/amx/eaton/blinds/0?command=open",
        "/amx/eaton/blinds/100?command=open",
        "/amx/eaton/blinds/0/status",
        "/amx/eaton/blinds/100/status",
        "/amx/eaton/lights/0?turn=on",
        "/amx/eaton/lights/100?turn=on",
        "/amx/eaton/lights/0/level?value=50",
        "/amx/eaton/lights/100/level?value=50"
    })
    void should_refuse_a_point_outside_1_to_99(final String uri) {
        webTestClient.post()
            .uri(uri)
            .exchange()
            .expectStatus().isBadRequest()
            .expectBody()
            .jsonPath("$.errors[0].message").isEqualTo("Invalid request parameter")
            .jsonPath("$.errors[0].details").isEqualTo("point must be between 1 and 99");

        verifyNoInteractions(eatonCommandService);
    }

    @ParameterizedTest
    @ValueSource(strings = {"-1", "101"})
    void should_refuse_a_level_outside_0_to_100(final String value) {
        webTestClient.post()
            .uri("/amx/eaton/lights/12/level?value={value}", value)
            .exchange()
            .expectStatus().isBadRequest()
            .expectBody()
            .jsonPath("$.errors[0].details").isEqualTo("value must be between 0 and 100");

        verifyNoInteractions(eatonCommandService);
    }

    //as written and nothing else: a typo moves nothing
    @ParameterizedTest
    @ValueSource(strings = {"OPEN", "up", "", "open,close"})
    void should_refuse_a_blinds_command_it_does_not_know(final String command) {
        webTestClient.post()
            .uri("/amx/eaton/blinds/5?command={command}", command)
            .exchange()
            .expectStatus().isBadRequest()
            .expectBody()
            .jsonPath("$.errors[0].message").isEqualTo("command must be open, close or stop");

        verifyNoInteractions(eatonCommandService);
    }

    //heating-service reads every turn that is not "on" as off; here a typo must not switch a light off
    @ParameterizedTest
    @ValueSource(strings = {"ON", "true", "1", "of", ""})
    void should_refuse_a_turn_that_is_neither_on_nor_off(final String turn) {
        webTestClient.post()
            .uri("/amx/eaton/lights/7?turn={turn}", turn)
            .exchange()
            .expectStatus().isBadRequest()
            .expectBody()
            .jsonPath("$.errors[0].message").isEqualTo("turn must be on or off");

        verifyNoInteractions(eatonCommandService);
    }

    @ParameterizedTest
    @ValueSource(strings = {
        "/amx/eaton/blinds/5",
        "/amx/eaton/lights/7",
        "/amx/eaton/lights/12/level",
        "/amx/eaton/blinds/five?command=open",
        "/amx/eaton/blinds/five/status",
        "/amx/eaton/lights/12/level?value=half"
    })
    void should_refuse_a_request_without_its_parameter_or_with_one_that_is_no_number(final String uri) {
        webTestClient.post()
            .uri(uri)
            .exchange()
            .expectStatus().isBadRequest();

        verifyNoInteractions(eatonCommandService);
    }

    @Test
    void should_answer_not_found_with_the_code_for_a_point_without_a_device() {
        when(eatonCommandService.moveBlinds(45, BlindsCommand.OPEN)).thenReturn(Mono.error(new EatonCommandException(
            HttpStatus.NOT_FOUND, CustomError.NOT_FOUND_DEVICE, "No Eaton device of kind: blinds registered on point: 45"
        )));

        webTestClient.post()
            .uri("/amx/eaton/blinds/45?command=open")
            .exchange()
            .expectStatus().isNotFound()
            .expectBody()
            .jsonPath("$.errors[0].code").isEqualTo("NOT_FOUND_DEVICE")
            .jsonPath("$.errors[0].message").isEqualTo("Device not found")
            .jsonPath("$.errors[0].details").isEqualTo("No Eaton device of kind: blinds registered on point: 45");

        assertThat(processorLog.list).extracting(ILoggingEvent::getLevel).containsExactly(Level.WARN);
    }

    //a point without a device is the caller's and a WARN; the frame server away, silent or refusing
    //is an outage, and the alerts look at ERROR
    @ParameterizedTest
    @CsvSource({
        "CONFLICT,        AMBIGUOUS_POINT,          WARN",
        "CONFLICT,        NOT_A_DIMMER,             WARN",
        "BAD_GATEWAY,     FRAME_SERVER_UNAVAILABLE, ERROR",
        "BAD_GATEWAY,     FRAME_REFUSED,            ERROR",
        "GATEWAY_TIMEOUT, FRAME_SERVER_TIMEOUT,     ERROR"
    })
    void should_answer_a_failed_command_with_its_status_and_code_and_log_it_at_the_level_of_its_kind(
        final HttpStatus status,
        final CustomError error,
        final String level
    ) {
        when(eatonCommandService.setLightLevel(anyInt(), anyInt()))
            .thenReturn(Mono.error(new EatonCommandException(status, error, "what went wrong")));

        webTestClient.post()
            .uri("/amx/eaton/lights/12/level?value=50")
            .exchange()
            .expectStatus().isEqualTo(status)
            .expectBody()
            .jsonPath("$.errors[0].code").isEqualTo(error.name())
            .jsonPath("$.errors[0].message").isEqualTo(error.getDescription())
            .jsonPath("$.errors[0].details").isEqualTo("what went wrong");

        assertThat(processorLog.list).extracting(ILoggingEvent::getLevel).containsExactly(Level.toLevel(level));
        verify(eatonCommandService).setLightLevel(12, 50);
    }

    //the registry of database-service failing behind a command: the status of that failure, as for a datagram
    @ParameterizedTest
    @CsvSource({"BAD_GATEWAY", "GATEWAY_TIMEOUT"})
    void should_answer_with_the_status_of_a_failed_registry_call(final HttpStatus status) {
        when(eatonCommandService.switchLight(anyInt(), anyBoolean()))
            .thenReturn(Mono.error(new ConfigurationCallException(status, Set.of())));

        webTestClient.post()
            .uri("/amx/eaton/lights/7?turn=on")
            .exchange()
            .expectStatus().isEqualTo(status);
    }

    @ParameterizedTest
    @ValueSource(strings = {"/amx/eaton/blinds/5?command=open", "/amx/eaton/blinds/5/status"})
    void should_not_answer_a_get(final String uri) {
        webTestClient.get()
            .uri(uri)
            .exchange()
            .expectStatus().isEqualTo(HttpStatus.METHOD_NOT_ALLOWED);

        verifyNoInteractions(eatonCommandService);
    }
}

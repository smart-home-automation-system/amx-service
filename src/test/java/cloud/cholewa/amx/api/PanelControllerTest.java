package cloud.cholewa.amx.api;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import cloud.cholewa.amx.config.ExceptionHandlerConfig;
import cloud.cholewa.amx.infrastructure.error.CustomError;
import cloud.cholewa.amx.infrastructure.error.PanelCommandException;
import cloud.cholewa.amx.infrastructure.error.processor.PanelCommandExceptionProcessor;
import cloud.cholewa.amx.service.PanelCommandService;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.webflux.test.autoconfigure.WebFluxTest;
import org.springframework.context.annotation.Import;
import org.springframework.http.HttpStatus;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.reactive.server.WebTestClient;
import reactor.core.publisher.Mono;

import java.time.LocalDateTime;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

//what a caller of the doorbell of the entrance panel gets (HAS-484): 202 with sentAt, or the status
//and the code of the cause - 503 while the Office master is away - and an ERROR line
@Import(ExceptionHandlerConfig.class)
@WebFluxTest(PanelController.class)
class PanelControllerTest {

    private static final LocalDateTime SENT_AT = LocalDateTime.of(2026, 11, 22, 18, 30, 5);

    private final ListAppender<ILoggingEvent> processorLog = new ListAppender<>();

    @Autowired
    private WebTestClient webTestClient;

    @MockitoBean
    private PanelCommandService panelCommandService;

    @BeforeEach
    void setUp() {
        processorLog.start();
        ((Logger) LoggerFactory.getLogger(PanelCommandExceptionProcessor.class)).addAppender(processorLog);
    }

    @AfterEach
    void tearDown() {
        ((Logger) LoggerFactory.getLogger(PanelCommandExceptionProcessor.class)).detachAppender(processorLog);
    }

    @Test
    void should_accept_the_doorbell_and_say_when_the_master_took_it() {
        when(panelCommandService.ring()).thenReturn(Mono.just(SENT_AT));

        webTestClient.post()
            .uri("/amx/panel/doorbell")
            .exchange()
            .expectStatus().isAccepted()
            .expectBody()
            .jsonPath("$.sentAt").isEqualTo("2026-11-22T18:30:05");
    }

    @ParameterizedTest
    @CsvSource({
        "SERVICE_UNAVAILABLE,PANEL_SERVER_UNAVAILABLE,AMX panel server unavailable",
        "GATEWAY_TIMEOUT,PANEL_SERVER_TIMEOUT,AMX panel server did not answer",
        "BAD_GATEWAY,PANEL_REFUSED,AMX panel server refused the command"
    })
    void should_answer_a_failed_doorbell_with_the_status_and_the_code_of_its_cause(
        final HttpStatus status,
        final CustomError error,
        final String message
    ) {
        when(panelCommandService.ring())
            .thenReturn(Mono.error(new PanelCommandException(status, error, "the details")));

        webTestClient.post()
            .uri("/amx/panel/doorbell")
            .exchange()
            .expectStatus().isEqualTo(status)
            .expectBody()
            .jsonPath("$.errors[0].code").isEqualTo(error.name())
            .jsonPath("$.errors[0].message").isEqualTo(message)
            .jsonPath("$.errors[0].details").isEqualTo("the details");

        assertThat(processorLog.list)
            .singleElement()
            .satisfies(event -> assertThat(event.getLevel()).isEqualTo(Level.ERROR));
    }
}

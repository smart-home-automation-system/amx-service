package cloud.cholewa.amx.api;

import cloud.cholewa.amx.config.ExceptionHandlerConfig;
import cloud.cholewa.amx.infrastructure.error.ConfigurationCallException;
import cloud.cholewa.amx.infrastructure.error.CustomError;
import cloud.cholewa.amx.infrastructure.error.EatonParsingException;
import cloud.cholewa.amx.service.AmxService;
import cloud.cholewa.home.model.EatonDatagramReply;
import cloud.cholewa.home.model.EatonGatewayType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.webflux.test.autoconfigure.WebFluxTest;
import org.springframework.context.annotation.Import;
import org.springframework.http.HttpStatus;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.reactive.server.WebTestClient;
import org.springframework.web.reactive.function.BodyInserters;
import reactor.core.publisher.Mono;

import java.util.Set;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

@Import(ExceptionHandlerConfig.class)
@WebFluxTest(AmxController.class)
class AmxControllerTest {

    @Autowired
    private WebTestClient webTestClient;

    @MockitoBean
    private AmxService amxService;

    @Test
    void should_successfully_process_message() {
        when(amxService.consumeAmxMessage(any()))
            .thenReturn(Mono.empty());

        webTestClient.post()
            .uri("/amx")
            .body(BodyInserters.fromValue(EatonDatagramReply.builder()
                .gateway(EatonGatewayType.BLINDS)
                .message("5A,C,C1,2C,62,3,0,1,58,0,0,43,5,A5")
                .build()))
            .exchange()
            .expectStatus().isOk();
    }

    @Test
    void should_return_error_when_processing_message_fails() {
        when(amxService.consumeAmxMessage(any()))
            .thenReturn(Mono.error(new EatonParsingException(CustomError.EXTRACTING_MESSAGE_ERROR)));

        webTestClient.post()
            .uri("/amx")
            .body(BodyInserters.fromValue(EatonDatagramReply.builder()
                .gateway(EatonGatewayType.BLINDS)
                .message("5A,C,C1,2C,62,3,0,1,58,0,0,43,5,A5")
                .build()))
            .exchange()
            .expectStatus()
            .isBadRequest();
    }

    //the HTTP status the AMX controller sees, which the unit test of the client alone does not show
    @ParameterizedTest
    @EnumSource(value = HttpStatus.class, names = {"NOT_FOUND", "BAD_GATEWAY", "GATEWAY_TIMEOUT"})
    void should_answer_with_the_status_of_a_failed_configuration_call(final HttpStatus status) {
        when(amxService.consumeAmxMessage(any()))
            .thenReturn(Mono.error(new ConfigurationCallException(status, Set.of())));

        webTestClient.post()
            .uri("/amx")
            .body(BodyInserters.fromValue(EatonDatagramReply.builder()
                .gateway(EatonGatewayType.BLINDS)
                .message("5A,C,C1,2C,62,3,0,1,58,0,0,43,5,A5")
                .build()))
            .exchange()
            .expectStatus()
            .isEqualTo(status);
    }
}

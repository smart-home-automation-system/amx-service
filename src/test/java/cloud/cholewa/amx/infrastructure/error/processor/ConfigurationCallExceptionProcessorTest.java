package cloud.cholewa.amx.infrastructure.error.processor;

import cloud.cholewa.amx.infrastructure.error.ConfigurationCallException;
import cloud.cholewa.commons.error.model.ErrorMessage;
import cloud.cholewa.commons.error.model.Errors;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpStatus;

import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

class ConfigurationCallExceptionProcessorTest {

    private final ConfigurationCallExceptionProcessor sut = new ConfigurationCallExceptionProcessor();

    @ParameterizedTest
    @EnumSource(value = HttpStatus.class, names = {"NOT_FOUND", "BAD_GATEWAY", "GATEWAY_TIMEOUT"})
    void should_answer_with_the_status_of_the_exception(final HttpStatus status) {
        final Errors errors = sut.apply(new ConfigurationCallException(
            status, Set.of(ErrorMessage.builder().message("m").details("d").build())
        ));

        assertThat(errors.getHttpStatus()).isEqualTo(status);
        assertThat(errors.getErrors()).extracting(ErrorMessage::getDetails).containsExactly("d");
    }

    @Test
    void should_answer_bad_gateway_when_the_exception_carries_no_status() {
        final Errors errors = sut.apply(new ConfigurationCallException(null, Set.of()));

        assertThat(errors.getHttpStatus()).isEqualTo(HttpStatus.BAD_GATEWAY);
    }

    @Test
    void should_fall_back_to_the_message_when_an_error_has_no_details() {
        final Errors errors = sut.apply(new ConfigurationCallException(
            HttpStatus.BAD_GATEWAY, Set.of(ErrorMessage.builder().message("boom").build())
        ));

        assertThat(errors.getErrors()).extracting(ErrorMessage::getDetails).containsExactly("boom");
    }
}

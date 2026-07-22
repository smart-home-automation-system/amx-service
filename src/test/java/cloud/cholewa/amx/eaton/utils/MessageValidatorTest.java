package cloud.cholewa.amx.eaton.utils;

import cloud.cholewa.amx.infrastructure.error.EatonParsingException;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

import static cloud.cholewa.amx.eaton.utils.MessageValidator.isValidEatonMessage;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertTrue;

class MessageValidatorTest {

    @ParameterizedTest(name = "{0}")
    @MethodSource("invalidMessages")
    void should_throw_exception_if_no_valid_eaton_message(
        final String name,
        final String message,
        final String errorMessage
    ) {
        assertThatThrownBy(() -> isValidEatonMessage(message))
            .isInstanceOf(EatonParsingException.class)
            .hasMessage(errorMessage);
    }

    private static Stream<Arguments> invalidMessages() {
        return Stream.of(
            Arguments.of(
                "unsupported message length",
                "C,C1,21,70,0,0,0,0,0,0,32,10",
                "Invalid Eaton message - supported length are 6, 8 and 12"
            ),
            Arguments.of(
                "missing message start and end",
                "C,C1,21,70,0,0,0,,44,22,0,0,32,10",
                "Invalid Eaton message - missing SOL or EOL"
            ),
            Arguments.of(
                "missing message start",
                "C,C1,21,70,0,0,0,32,10,A5",
                "Invalid Eaton message - missing SOL or EOL"
            ),
            Arguments.of(
                "missing message end",
                "5A,C,C1,21,70,0,32,10",
                "Invalid Eaton message - missing SOL or EOL"
            ),
            Arguments.of(
                "contains not only hex values",
                "5A,K,C1,21,70,0,0,0,0,AA,bb,32,10,A5",
                "Invalid Eaton message - contains non hex values"
            ),
            Arguments.of(
                "contains hex values larger than FF",
                "5A,C,C1,21,70,aa,bb,0,0,0,32,199,10,A5",
                "Invalid Eaton message - contains non hex values"
            ),
            Arguments.of(
                "contains subsequent commas",
                "5A,C,C1,21,70,0,0,0,0,0,32,,10,A5",
                "Invalid Eaton message - contains non hex values"
            )
        );
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("validMessages")
    void should_not_throw_exception_if_message_is_valid(final String name, final String validMessage) {
        assertTrue(isValidEatonMessage(validMessage));
    }

    private static Stream<Arguments> validMessages() {
        return Stream.of(
            Arguments.of("PayloadType.TX valid message 6 + 2 bytes", "5A,C,C1,21,70,32,10,A5"),
            Arguments.of("PayloadType.RX valid message 12 + 2 bytes", "5A,C,C1,21,70,AA,BB,0,1A,B6,0,32,10,A5"),
            Arguments.of("PayloadType.STATUS valid message 8 + 2 bytes", "5A,C,C1,21,CC,DE,70,32,10,A5")
        );
    }
}
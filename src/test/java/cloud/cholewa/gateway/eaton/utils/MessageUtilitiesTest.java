package cloud.cholewa.gateway.eaton.utils;

import cloud.cholewa.gateway.infrastructure.error.EatonParsingException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.List;
import java.util.stream.Stream;

import static cloud.cholewa.gateway.eaton.utils.MessageUtilities.extractDataPoint;
import static cloud.cholewa.gateway.eaton.utils.MessageUtilities.extractMessage;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertEquals;

class MessageUtilitiesTest {

    @Test
    void should_throw_exception_if_message_is_empty() {
        assertThatThrownBy(() -> extractMessage(""))
            .isInstanceOf(EatonParsingException.class)
            .hasMessage("Error during extracting message");
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("testMessages")
    void should_not_throw_exception_if_message_is_valid(
        final String name,
        final String message,
        final List<String> result
    ) {
        assertEquals(result, extractMessage(message));
    }

    private static Stream<Arguments> testMessages() {
        return Stream.of(
            Arguments.of(
                "valid short message",
                "5A,8,C3,1C,4,0,0,0,0,A5",
                List.of("8", "C3", "1C", "4", "0", "0", "0", "0")
            ),
            Arguments.of(
                "valid long message",
                "5A,C,C1,2C,62,3,0,1,58,0,0,43,5,A5",
                List.of("C", "C1", "2C", "62", "3", "0", "1", "58", "0", "0", "43", "5")
            ),
            Arguments.of(
                "SOL and EOL in middle",
                "5A,C,C1,11,70,0,5A,0,A5,0,0,44,10,A5",
                List.of("C", "C1", "11", "70", "0", "5A", "0", "A5", "0", "0", "44", "10")
            )
        );
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("extract_data_point_correct_message")
    void should_extract_data_point(
        final String name,
        final List<String> message,
        final int expected
    ) {
        assertEquals(expected, extractDataPoint(message));
    }

    private static Stream<Arguments> extract_data_point_correct_message() {
        return Stream.of(
            Arguments.of(
                "rx message",
                List.of("C", "C1", "29", "62", "3", "0", "0", "CD", "0", "0", "34", "5"),
                41
            ),
            Arguments.of(
                "status message",
                List.of("8", "C3", "3", "1", "1", "1", "1", "1"),
                3
            )
        );
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("extract_data_point_incorrect_message")
    void should_throw_exception_when_incorrect_message(
        final String name,
        final List<String> message,
        final String expected
    ) {
        assertThatThrownBy(() -> extractDataPoint(message))
            .isInstanceOf(EatonParsingException.class)
            .hasMessage(expected);
    }

    private static Stream<Arguments> extract_data_point_incorrect_message() {
        return Stream.of(
            Arguments.of(
                "rx message too short",
                List.of("C", "C1", "29", "62", "3", "0", "0", "0", "0", "34", "5"),
                "Message length mismatch, message length differ to expected from first byte"
            ),
            Arguments.of(
                "rx message too long",
                List.of("C", "C1", "29", "62", "3", "0", "0", "CD", "0", "0", "34", "5", "8"),
                "Message length mismatch, message length differ to expected from first byte"
            ),
            Arguments.of(
                "rx message data point too small",
                List.of("C", "C1", "0", "62", "3", "0", "0", "CD", "0", "0", "34", "5"),
                "Invalid data point number"
            ),
            Arguments.of(
                "rx message data point too big",
                List.of("C", "C1", "129", "62", "3", "0", "0", "CD", "0", "0", "34", "5"),
                "Invalid data point number"
            ),
            Arguments.of(
                "rx message data point not numerical",
                List.of("C", "C1", "AA", "62", "3", "0", "0", "CD", "0", "0", "34", "5"),
                "Invalid data point number"
            ),
            Arguments.of(
                "status message too short",
                List.of("8", "C3", "3", "1", "1", "1", "1"),
                "Message length mismatch, message length differ to expected from first byte"
            ),
            Arguments.of(
                "status message too long",
                List.of("8", "C3", "3", "1", "1", "1", "1", "1", "7"),
                "Message length mismatch, message length differ to expected from first byte"
            ),
            Arguments.of(
                "status message data point too small",
                List.of("8", "C3", "-1", "1", "1", "1", "1", "1"),
                "Invalid data point number"
            ),
            Arguments.of(
                "status message data point too big",
                List.of("8", "C3", "312", "1", "1", "1", "1", "1"),
                "Invalid data point number"
            ),
            Arguments.of(
                "status message data point not numerical",
                List.of("8", "C3", "D3", "1", "1", "1", "1", "1"),
                "Invalid data point number"
            )
        );
    }
}
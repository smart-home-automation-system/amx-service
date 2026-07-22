package cloud.cholewa.amx.eaton.utils.device;

import cloud.cholewa.amx.infrastructure.error.EatonParsingException;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.List;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TemperatureParserTest {

    @ParameterizedTest(name = "{0}")
    @MethodSource("message_length_is_not_12")
    void throw_exception_if_message_length_is_not_12(final String name, final List<String> elements) {

        assertThatThrownBy(() -> TemperatureParser.calculateRoomTemperature(elements))
            .isInstanceOf(EatonParsingException.class)
            .hasMessage("Invalid message length for temperature sensor must be 12");
    }

    private static Stream<Arguments> message_length_is_not_12() {
        return Stream.of(
            Arguments.of(
                "not enough values",
                List.of("5A", "C1", "C")
            ),
            Arguments.of(
                "too many values",
                List.of("5A", "C", "C1", "36", "62", "17", "0", "0", "CF", "0", "4", "44", "5", "00", "A5")
            )
        );
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("valid_messages")
    void should_return_temperature(
        final String name,
        final List<String> elements,
        final double expectedTemperature
    ) {
        assertThat(TemperatureParser.calculateRoomTemperature(elements))
            .isEqualTo(expectedTemperature);
    }

    private static Stream<Arguments> valid_messages() {
        return Stream.of(
            Arguments.of(
                "valid temperature under 25.5",
                List.of("C", "C1", "36", "62", "17", "0", "0", "CF", "0", "4", "44", "5"),
                20.7
            ),
            Arguments.of(
                "negative temperature",
                List.of("C", "C1", "2D", "62", "3", "0", "FF", "DB", "0", "0", "4E", "5"),
                -3.7
            ),
            Arguments.of(
                "valid temperature equals 25.5",
                List.of("C", "C1", "36", "62", "17", "0", "0", "FF", "0", "4", "44", "5"),
                25.5
            ),
            Arguments.of(
                "valid temperature over 25.5",
                List.of("C", "C1", "36", "62", "17", "0", "01", "97", "0", "4", "44", "5"),
                40.7
            )
        );
    }
}
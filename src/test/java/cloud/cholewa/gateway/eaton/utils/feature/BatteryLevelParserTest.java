package cloud.cholewa.gateway.eaton.utils.feature;

import cloud.cholewa.gateway.eaton.model.BatteryLevel;
import cloud.cholewa.gateway.infrastructure.error.EatonParsingException;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class BatteryLevelParserTest {

    @ParameterizedTest(name = "{0}")
    @MethodSource("validBatteryValues")
    void should_return_valid_battery_level(
        final String name,
        final String batteryLevel,
        final BatteryLevel expectedLevel
    ) {
        assertThat(BatteryLevelParser.getBatteryLevel(batteryLevel))
            .isEqualTo(expectedLevel);
    }

    private static Stream<Arguments> validBatteryValues() {
        return Stream.of(
            Arguments.of(
                "not available",
                "0",
                BatteryLevel.NOT_AVAILABLE
            ),
            Arguments.of(
                "empty",
                "1",
                BatteryLevel.EMPTY
            ),
            Arguments.of(
                "very weak",
                "2",
                BatteryLevel.VERY_WEAK
            ),
            Arguments.of(
                "weak",
                "3",
                BatteryLevel.WEAK
            ),
            Arguments.of(
                "good",
                "4",
                BatteryLevel.GOOD
            ),
            Arguments.of(
                "new",
                "5",
                BatteryLevel.NEW
            ),
            Arguments.of(
                "mains operated",
                "10",
                BatteryLevel.MAINS_OPERATED
            )
        );
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("invalidBatteryValues")
    void should_throw_exception_for_invalid_values(final String name, final String batteryLevel) {
        assertThatThrownBy(() -> BatteryLevelParser.getBatteryLevel(batteryLevel))
            .isInstanceOf(EatonParsingException.class)
            .hasMessage("Invalid value of battery level");
    }

    private static Stream<Arguments> invalidBatteryValues() {
        return Stream.of(
            Arguments.of("invalid value 06", "06"),
            Arguments.of("invalid value 0F", "0F"),
            Arguments.of("invalid value 11", "11"),
            Arguments.of("invalid value E4", "E4"),
            Arguments.of("invalid value ff", "ff"),
            Arguments.of("invalid value 106", "106")
        );
    }
}
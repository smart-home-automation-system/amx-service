package cloud.cholewa.amx.eaton.frame;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

//The expected frames are the ones the NetLinx module Eaton_xComfort_Comm_nl3_0_0.axs writes to a
//gateway, quoted next to each test: "$5A,$09,$B1, <port>, <event>, <data>, $00,$00,$00,$00,$A5",
//where the port of the virtual device is the data point
class EatonFrameBuilderTest {

    //MOTOR_OPEN:  $5A,$09,$B1, CHANNEL.DEVICE.PORT, $0E,$01,$00,$00,$00,$00,$A5
    //MOTOR_CLOSE: $5A,$09,$B1, CHANNEL.DEVICE.PORT, $0E,$00,$00,$00,$00,$00,$A5
    //MOTOR_STOP:  $5A,$09,$B1, CHANNEL.DEVICE.PORT, $0E,$02,$00,$00,$00,$00,$A5
    @ParameterizedTest
    @CsvSource(delimiter = '|', value = {
        "OPEN  | 5A,09,B1,05,0E,01,00,00,00,00,A5",
        "CLOSE | 5A,09,B1,05,0E,00,00,00,00,00,A5",
        "STOP  | 5A,09,B1,05,0E,02,00,00,00,00,A5"
    })
    void should_build_the_frame_that_moves_blinds(final BlindsCommand command, final String frame) {
        assertThat(EatonFrameBuilder.blinds(5, command)).isEqualTo(frame);
    }

    //PWR_ON:  $5A,$09,$B1, CHANNEL.DEVICE.PORT, $0A,$01,$00,$00,$00,$00,$A5
    //PWR_OFF: $5A,$09,$B1, CHANNEL.DEVICE.PORT, $0A,$00,$00,$00,$00,$00,$A5
    @ParameterizedTest
    @CsvSource(delimiter = '|', value = {
        "true  | 5A,09,B1,07,0A,01,00,00,00,00,A5",
        "false | 5A,09,B1,07,0A,00,00,00,00,00,A5"
    })
    void should_build_the_frame_that_switches_a_light(final boolean on, final String frame) {
        assertThat(EatonFrameBuilder.lightSwitch(7, on)).isEqualTo(frame);
    }

    //BRIGHT_LVL: $5A,$09,$B1, LEVEL.INPUT.DEVICE.PORT, $0C, LEVEL.VALUE, $00,$00,$00,$00,$A5
    //the module sends a level of 0 to 100, as the byte it is
    @ParameterizedTest
    @CsvSource(delimiter = '|', value = {
        "0   | 5A,09,B1,0C,0C,00,00,00,00,00,A5",
        "9   | 5A,09,B1,0C,0C,09,00,00,00,00,A5",
        "50  | 5A,09,B1,0C,0C,32,00,00,00,00,A5",
        "100 | 5A,09,B1,0C,0C,64,00,00,00,00,A5"
    })
    void should_build_the_frame_that_sets_the_level_of_a_dimmer(final int level, final String frame) {
        assertThat(EatonFrameBuilder.lightLevel(12, level)).isEqualTo(frame);
    }

    //the status request of the module: $5A,$09,$B1, idDevice, $0B,$00,$00,$00,$00,$00,$A5
    @Test
    void should_build_the_frame_that_asks_for_the_status() {
        assertThat(EatonFrameBuilder.statusRequest(33)).isEqualTo("5A,09,B1,21,0B,00,00,00,00,00,A5");
    }

    //a point is one byte in two hex digits, in capitals: 10 is 0A, never A or 0a
    @ParameterizedTest
    @CsvSource(delimiter = '|', value = {
        "1  | 5A,09,B1,01,0A,01,00,00,00,00,A5",
        "10 | 5A,09,B1,0A,0A,01,00,00,00,00,A5",
        "45 | 5A,09,B1,2D,0A,01,00,00,00,00,A5",
        "99 | 5A,09,B1,63,0A,01,00,00,00,00,A5"
    })
    void should_write_the_point_as_one_byte_in_two_hex_digits(final int point, final String frame) {
        assertThat(EatonFrameBuilder.lightSwitch(point, true)).isEqualTo(frame);
    }

    @ParameterizedTest
    @ValueSource(ints = {-1, 0, 100, 255})
    void should_refuse_a_point_outside_1_to_99(final int point) {
        assertThatIllegalArgumentException().isThrownBy(() -> EatonFrameBuilder.blinds(point, BlindsCommand.OPEN));
        assertThatIllegalArgumentException().isThrownBy(() -> EatonFrameBuilder.lightSwitch(point, true));
        assertThatIllegalArgumentException().isThrownBy(() -> EatonFrameBuilder.lightLevel(point, 50));
        assertThatIllegalArgumentException().isThrownBy(() -> EatonFrameBuilder.statusRequest(point));
    }

    @ParameterizedTest
    @ValueSource(ints = {-1, 101, 255})
    void should_refuse_a_level_outside_0_to_100(final int level) {
        assertThatIllegalArgumentException().isThrownBy(() -> EatonFrameBuilder.lightLevel(12, level));
    }

    @ParameterizedTest
    @CsvSource({"open,OPEN", "close,CLOSE", "stop,STOP"})
    void should_find_a_blinds_command_by_its_parameter(final String parameter, final BlindsCommand command) {
        assertThat(BlindsCommand.ofParameter(parameter)).contains(command);
    }

    //as written and nothing else: a typo is refused, not guessed
    @ParameterizedTest
    @ValueSource(strings = {"OPEN", "Open", " open", "up", ""})
    void should_know_no_blinds_command_by_another_spelling(final String parameter) {
        assertThat(BlindsCommand.ofParameter(parameter)).isEmpty();
    }
}

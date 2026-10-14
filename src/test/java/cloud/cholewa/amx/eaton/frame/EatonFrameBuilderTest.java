package cloud.cholewa.amx.eaton.frame;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

//The expected frames are the TX packet of the gateway's protocol as documented - 5A, length 06,
//B1, the data point, the event, the data, the priority 00, A5 - verified on the Boiler blind (point
//14) and the Boiler light (point 35) of the house through the frame server on 2026-10-14. The
//length 09 with four bytes of padding that the NetLinx module behind the panels sends is the
//hybrid of 2017; the gateways accept both
class EatonFrameBuilderTest {

    //0E: blinds move - 01 open, 00 close, 02 stop
    @ParameterizedTest
    @CsvSource(delimiter = '|', value = {
        "OPEN  | 5A,06,B1,05,0E,01,00,A5",
        "CLOSE | 5A,06,B1,05,0E,00,00,A5",
        "STOP  | 5A,06,B1,05,0E,02,00,A5"
    })
    void should_build_the_frame_that_moves_blinds(final BlindsCommand command, final String frame) {
        assertThat(EatonFrameBuilder.blinds(5, command)).isEqualTo(frame);
    }

    //0A: switch - 01 on, 00 off
    @ParameterizedTest
    @CsvSource(delimiter = '|', value = {
        "true  | 5A,06,B1,07,0A,01,00,A5",
        "false | 5A,06,B1,07,0A,00,00,A5"
    })
    void should_build_the_frame_that_switches_a_light(final boolean on, final String frame) {
        assertThat(EatonFrameBuilder.lightSwitch(7, on)).isEqualTo(frame);
    }

    //0C: dim level in percent, 0 to 100, as the byte it is
    @ParameterizedTest
    @CsvSource(delimiter = '|', value = {
        "0   | 5A,06,B1,0C,0C,00,00,A5",
        "9   | 5A,06,B1,0C,0C,09,00,A5",
        "50  | 5A,06,B1,0C,0C,32,00,A5",
        "100 | 5A,06,B1,0C,0C,64,00,A5"
    })
    void should_build_the_frame_that_sets_the_level_of_a_dimmer(final int level, final String frame) {
        assertThat(EatonFrameBuilder.lightLevel(12, level)).isEqualTo(frame);
    }

    //0B: status request, data 00
    @Test
    void should_build_the_frame_that_asks_for_the_status() {
        assertThat(EatonFrameBuilder.statusRequest(33)).isEqualTo("5A,06,B1,21,0B,00,00,A5");
    }

    //a point is one byte in two hex digits, in capitals: 10 is 0A, never A or 0a
    @ParameterizedTest
    @CsvSource(delimiter = '|', value = {
        "1  | 5A,06,B1,01,0A,01,00,A5",
        "10 | 5A,06,B1,0A,0A,01,00,A5",
        "45 | 5A,06,B1,2D,0A,01,00,A5",
        "99 | 5A,06,B1,63,0A,01,00,A5"
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

package cloud.cholewa.amx.eaton.utils.device;

import cloud.cholewa.amx.eaton.model.RxEventType;
import cloud.cholewa.amx.infrastructure.error.EatonParsingException;
import cloud.cholewa.home.model.DeviceEvent;
import cloud.cholewa.home.model.EatonGatewayType;
import cloud.cholewa.home.model.EatonRxEvent;
import cloud.cholewa.home.model.SmartVendor;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.List;

import static cloud.cholewa.amx.eaton.utils.MessageUtilities.extractMessage;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class DeviceEventParserTest {

    private static final LocalDateTime AT = LocalDateTime.of(2026, 10, 15, 18, 30, 5);

    //the two enums share names, not order: every message type this service knows has an event of
    //the same name, or a frame of it would fail at the first press of a button
    @ParameterizedTest
    @EnumSource(RxEventType.class)
    void should_have_an_event_for_every_message_type(final RxEventType type) {
        assertThat(EatonRxEvent.valueOf(type.name())).isNotNull();
    }

    @ParameterizedTest
    @CsvSource({
        "50, PUSH_BUTTON_ON",
        "51, PUSH_BUTTON_OFF",
        "52, SWITCH_ON",
        "53, SWITCH_OFF",
        "54, UP_PRESSED",
        "55, UP_RELEASED",
        "56, DOWN_PRESSED",
        "57, DOWN_RELEASED"
    })
    void should_read_an_event_without_a_value(final String type, final EatonRxEvent event) {
        assertThat(parse("5A,0C,C1,0E," + type + ",00,01,00,00,00,00,3A,10,A5", EatonGatewayType.BLINDS))
            .isEqualTo(DeviceEvent.builder()
                .at(AT)
                .vendor(SmartVendor.EATON)
                .gateway(EatonGatewayType.BLINDS)
                .point(14)
                .event(event)
                .build());
    }

    //the status of an actuator is its info short: a blind stop / open / close, a switch off / on,
    //a dimmer its level in percent
    @ParameterizedTest
    @CsvSource({
        "00, 0",
        "01, 1",
        "02, 2",
        "64, 100"
    })
    void should_read_the_status_of_an_actuator_from_the_info_short(final String infoShort, final int value) {
        final DeviceEvent event = parse("5A,0C,C1,23,70,00," + infoShort + ",00,00,00,00,3A,10,A5", EatonGatewayType.LIGHTS);

        assertThat(event.getEvent()).isEqualTo(EatonRxEvent.STATUS);
        assertThat(event.getGateway()).isEqualTo(EatonGatewayType.LIGHTS);
        assertThat(event.getPoint()).isEqualTo(35);
        assertThat(event.getValue()).isEqualTo(value);
    }

    //the HTTP forwarder of old wrote no leading zeros: the bytes are read as numbers
    @Test
    void should_read_a_frame_without_leading_zeros() {
        final DeviceEvent event = parse("5A,C,C1,E,70,0,2,0,0,0,0,3A,10,A5", EatonGatewayType.BLINDS);

        assertThat(event.getPoint()).isEqualTo(14);
        assertThat(event.getValue()).isEqualTo(2);
    }

    @ParameterizedTest
    @CsvSource({
        //a percent is a byte of 0..255
        "01,00,00, 0",
        "01,80,00, 50",
        "01,FF,00, 100",
        //an unsigned short, high byte first
        "0D,01,2C, 300",
        "0D,FF,FF, 65535"
    })
    void should_read_an_analog_value_that_is_a_whole_number(
        final String dataType, final String high, final String low, final int value
    ) {
        final DeviceEvent event =
            parse("5A,0C,C1,46,62," + dataType + ",00," + high + "," + low + ",00,00,3A,10,A5", EatonGatewayType.BLINDS);

        assertThat(event.getEvent()).isEqualTo(EatonRxEvent.ANALOG_VALUE);
        assertThat(event.getValue()).isEqualTo(value);
    }

    //a temperature, a float or the data of a room controller is no whole number: not this parser's
    @ParameterizedTest
    @ValueSource(strings = {"00", "03", "04", "17"})
    void should_refuse_an_analog_value_that_is_not_a_whole_number(final String dataType) {
        assertThatThrownBy(() -> parse("5A,0C,C1,46,62," + dataType + ",00,00,F0,00,00,3A,10,A5", EatonGatewayType.BLINDS))
            .isInstanceOf(EatonParsingException.class)
            .hasMessage("Analog value of a device is not a whole number");
    }

    //too cold, too warm, a basic-mode confirmation, a forced value: the gateway knows them, the
    //services of the devices do not
    @ParameterizedTest
    @ValueSource(strings = {"59", "5A", "5B", "61", "63", "64", "80"})
    void should_refuse_a_message_type_without_an_event(final String type) {
        assertThatThrownBy(() -> parse("5A,0C,C1,0E," + type + ",00,00,00,00,00,00,3A,10,A5", EatonGatewayType.BLINDS))
            .isInstanceOf(EatonParsingException.class)
            .hasMessage("Message type of a device not published as an event");
    }

    //a TX frame - a command echoed - has a data point too, and is no event of a device
    @Test
    void should_refuse_a_frame_that_is_not_an_rx_frame() {
        assertThatThrownBy(() -> parse("5A,06,B1,0E,0E,01,00,A5", EatonGatewayType.BLINDS))
            .isInstanceOf(EatonParsingException.class)
            .hasMessage("Invalid message of a device: an RX frame of 12 bytes expected");
    }

    @Test
    void should_refuse_an_rx_frame_of_another_length() {
        final List<String> elements = Arrays.asList("0B", "C1", "0E", "70", "00", "01", "00", "00", "00", "00", "3A");

        assertThatThrownBy(() -> DeviceEventParser.parse(elements, EatonGatewayType.BLINDS, AT))
            .isInstanceOf(EatonParsingException.class)
            .hasMessage("Invalid message of a device: an RX frame of 12 bytes expected");
    }

    @Test
    void should_refuse_a_data_point_out_of_range() {
        assertThatThrownBy(() -> parse("5A,0C,C1,64,70,00,01,00,00,00,00,3A,10,A5", EatonGatewayType.BLINDS))
            .isInstanceOf(EatonParsingException.class)
            .hasMessage("Invalid data point number");
    }

    private static DeviceEvent parse(final String frame, final EatonGatewayType gateway) {
        return DeviceEventParser.parse(extractMessage(frame), gateway, AT);
    }
}

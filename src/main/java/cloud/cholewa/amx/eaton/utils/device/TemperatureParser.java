package cloud.cholewa.amx.eaton.utils.device;

import cloud.cholewa.amx.infrastructure.error.EatonParsingException;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;

import java.util.List;

import static cloud.cholewa.amx.infrastructure.error.CustomError.TEMPERATURE_SENSOR_INVALID_DEVICE_TYPE;
import static cloud.cholewa.amx.infrastructure.error.CustomError.TEMPERATURE_SENSOR_INVALID_MESSAGE_LENGTH;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class TemperatureParser {

    //the bytes are compared as numbers: the frame server writes every byte in two digits (03),
    //the HTTP forwarder of old wrote none that were not needed (3)
    private static final int TEMPERATURE_SENSOR_TYPE = 0x62;
    private static final int ANALOG_SENSOR_TYPE = 0x03;
    private static final int ROOM_CONTROLLER_SENSOR_TYPE = 0x17;

    /*
     * A valid message type is PayloadType.RX 12-byte length
     * */
    public static double calculateRoomTemperature(final List<String> elements) {

        throwExceptionWhenInvalidMessageLength(elements);
        throwExceptionWhenDeviceIsNotTemperatureSensor(elements);
        throwExceptionWhenDeviceIsNotSourceOfTemperature(elements);

        return getTemperature(elements.get(6), elements.get(7));
    }

    private static Double getTemperature(final String oldByte, final String youngByte) {
        double old = Integer.parseInt(oldByte, 16);
        double young = Integer.parseInt(youngByte, 16);

        return oldByte.equals("FF") ? (young - 256) / 10 : (256 * old + young) / 10;
    }

    /*
     * Valid Eaton RX_packet must be 12 bytes long
     **/
    private static void throwExceptionWhenInvalidMessageLength(final List<String> elements) {
        if (elements.size() != 12) {
            throw new EatonParsingException(TEMPERATURE_SENSOR_INVALID_MESSAGE_LENGTH);
        }
    }

    private static void throwExceptionWhenDeviceIsNotTemperatureSensor(final List<String> elements) {
        if (byteOf(elements.get(3)) != TEMPERATURE_SENSOR_TYPE) {
            throw new EatonParsingException(TEMPERATURE_SENSOR_INVALID_DEVICE_TYPE);
        }
    }

    private static void throwExceptionWhenDeviceIsNotSourceOfTemperature(final List<String> elements) {
        final int dataType = byteOf(elements.get(4));
        if (dataType != ROOM_CONTROLLER_SENSOR_TYPE && dataType != ANALOG_SENSOR_TYPE) {
            throw new EatonParsingException(TEMPERATURE_SENSOR_INVALID_DEVICE_TYPE);
        }
    }

    //the elements passed the validator: hex digits, one or two of them
    private static int byteOf(final String element) {
        return Integer.parseInt(element, 16);
    }
}

package cloud.cholewa.gateway.eaton.utils.device;

import cloud.cholewa.gateway.infrastructure.error.EatonParsingException;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;

import java.util.List;

import static cloud.cholewa.gateway.infrastructure.error.ErrorDictionary.TEMPERATURE_SENSOR_INVALID_DEVICE_TYPE;
import static cloud.cholewa.gateway.infrastructure.error.ErrorDictionary.TEMPERATURE_SENSOR_INVALID_MESSAGE_LENGTH;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class TemperatureParser {

    private static final String TEMPERATURE_SENSOR_TYPE = "62";
    private static final String ANALOG_SENSOR_TYPE = "3";
    private static final String ROOM_CONTROLLER_SENSOR_TYPE = "17";

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
        if (!elements.get(3).equals(TEMPERATURE_SENSOR_TYPE)) {
            throw new EatonParsingException(TEMPERATURE_SENSOR_INVALID_DEVICE_TYPE);
        }
    }

    private static void throwExceptionWhenDeviceIsNotSourceOfTemperature(final List<String> elements) {
        if (!elements.get(4).equals(ROOM_CONTROLLER_SENSOR_TYPE) && !elements.get(4).equals(ANALOG_SENSOR_TYPE)) {
            throw new EatonParsingException(TEMPERATURE_SENSOR_INVALID_DEVICE_TYPE);
        }
    }
}

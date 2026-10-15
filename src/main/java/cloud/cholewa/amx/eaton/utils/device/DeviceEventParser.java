package cloud.cholewa.amx.eaton.utils.device;

import cloud.cholewa.amx.eaton.model.RxEventType;
import cloud.cholewa.amx.infrastructure.error.EatonParsingException;
import cloud.cholewa.home.model.DeviceEvent;
import cloud.cholewa.home.model.EatonGatewayType;
import cloud.cholewa.home.model.EatonRxEvent;
import cloud.cholewa.home.model.SmartVendor;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Locale;

import static cloud.cholewa.amx.eaton.utils.MessageUtilities.extractDataPoint;
import static cloud.cholewa.amx.infrastructure.error.CustomError.DEVICE_EVENT_INVALID_MESSAGE;
import static cloud.cholewa.amx.infrastructure.error.CustomError.DEVICE_EVENT_UNKNOWN_TYPE;
import static cloud.cholewa.amx.infrastructure.error.CustomError.DEVICE_EVENT_VALUE_NOT_AN_INTEGER;

/**
 * Reads the RX frame of an Eaton device that is not a temperature sensor into the event the
 * services of the devices consume. The layout is {@code moe_protocol.h} of the gateway (the
 * {@code CLAUDE.md} of the {@code amx} repository has it): after the frame of
 * {@link cloud.cholewa.amx.eaton.utils.MessageUtilities#extractMessage} lost its start and stop
 * bytes, {@code 0} is the length, {@code 1} the type ({@code C1}), {@code 2} the data point,
 * {@code 3} the message type, {@code 4} the data type, {@code 5} the info short, {@code 6..9} the
 * data, high byte first, {@code 10} the RSSI and {@code 11} the battery.
 * <p>
 * Only {@code status} and {@code analog value} carry a number. The status of an actuator is its
 * info short: a blind {@code 0} stop, {@code 1} open, {@code 2} close; a switch {@code 0} off,
 * {@code 1} on; a dimmer its level in percent. An analog value is read by its data type, as long
 * as it is a whole number: a percent (a byte of {@code 0..255}, published as {@code 0..100}) or an
 * unsigned short. A temperature or a float is not this parser's - the sensors that send them are
 * read by {@link TemperatureParser} or not at all.
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class DeviceEventParser {

    private static final int LENGTH = 12;
    private static final int RX_TYPE = 0xC1;
    private static final int PERCENT = 0x01;
    private static final int UNSIGNED_SHORT = 0x0D;

    private static final int TYPE_INDEX = 1;
    private static final int MESSAGE_TYPE_INDEX = 3;
    private static final int DATA_TYPE_INDEX = 4;
    private static final int INFO_SHORT_INDEX = 5;
    private static final int DATA_INDEX = 6;

    public static DeviceEvent parse(final List<String> elements, final EatonGatewayType gateway, final LocalDateTime at) {
        if (elements.size() != LENGTH || byteOf(elements.get(TYPE_INDEX)) != RX_TYPE) {
            throw new EatonParsingException(DEVICE_EVENT_INVALID_MESSAGE);
        }
        final RxEventType type = typeOf(elements.get(MESSAGE_TYPE_INDEX));

        return DeviceEvent.builder()
            .at(at)
            .vendor(SmartVendor.EATON)
            .gateway(gateway)
            .point(extractDataPoint(elements))
            //the two enums share the names, not the order
            .event(EatonRxEvent.valueOf(type.name()))
            .value(valueOf(type, elements))
            .build();
    }

    //a message type the gateway knows and the services do not (too cold, too warm, a basic-mode
    //confirmation) is refused, not published under a name it does not have
    private static RxEventType typeOf(final String element) {
        try {
            return RxEventType.fromValue(String.format(Locale.ROOT, "%02X", byteOf(element)));
        } catch (IllegalArgumentException e) {
            throw new EatonParsingException(DEVICE_EVENT_UNKNOWN_TYPE);
        }
    }

    private static Integer valueOf(final RxEventType type, final List<String> elements) {
        return switch (type) {
            case STATUS -> byteOf(elements.get(INFO_SHORT_INDEX));
            case ANALOG_VALUE -> analogValue(elements);
            default -> null;
        };
    }

    private static int analogValue(final List<String> elements) {
        final int high = byteOf(elements.get(DATA_INDEX));
        return switch (byteOf(elements.get(DATA_TYPE_INDEX))) {
            case PERCENT -> Math.round(high * 100f / 255);
            case UNSIGNED_SHORT -> high * 256 + byteOf(elements.get(DATA_INDEX + 1));
            default -> throw new EatonParsingException(DEVICE_EVENT_VALUE_NOT_AN_INTEGER);
        };
    }

    //the elements passed the validator: hex digits, one or two of them. Compared as numbers - the
    //frame server writes every byte in two digits, the HTTP forwarder of old did not
    private static int byteOf(final String element) {
        return Integer.parseInt(element, 16);
    }
}

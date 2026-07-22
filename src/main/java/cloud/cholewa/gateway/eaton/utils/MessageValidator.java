package cloud.cholewa.gateway.eaton.utils;

import cloud.cholewa.gateway.infrastructure.error.EatonParsingException;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.Strings;

import java.util.regex.Pattern;

import static cloud.cholewa.gateway.eaton.model.Message.EOL;
import static cloud.cholewa.gateway.eaton.model.Message.SOL;
import static cloud.cholewa.gateway.infrastructure.error.CustomError.INVALID_MESSAGE_LENGTH;
import static cloud.cholewa.gateway.infrastructure.error.CustomError.MISSING_SOL_OR_EOL;
import static cloud.cholewa.gateway.infrastructure.error.CustomError.NON_HEX_VALUES;

@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class MessageValidator {

    private static final Pattern HEX = Pattern.compile("[A-F0-9]{1,2}");

    public static boolean isValidEatonMessage(final String message) {
        final String[] messageParts = message.split(",");
        throwExceptionWhenUnknownMessageLength(messageParts);
        throwExceptionWhenInvalidStartOrEndByte(message);
        throwExceptionWhenInvalidHexValues(messageParts);
        return true;
    }

    /*
     * Supported message length are:
     * 1. PayloadType.TX - B1 - 6 bytes
     * 2. PayloadType.RX - C1 - 12 bytes
     * 3. PayloadType.STATUS - C3 - 8 bytes
     * There are an additional two bytes for SOL and EOL
     * */
    private static void throwExceptionWhenUnknownMessageLength(final String[] messageParts) {
        if (messageParts.length == 14 || messageParts.length == 10 || messageParts.length == 8) {
            return;
        }
        throw new EatonParsingException(INVALID_MESSAGE_LENGTH);
    }

    /*
     * Eaton message always must starts with SOL(5a) and ends with EOL(a5)
     * */
    private static void throwExceptionWhenInvalidStartOrEndByte(final String message) {
        if (!Strings.CI.startsWith(message, SOL.getValue()) || !Strings.CI.endsWith(message, EOL.getValue())) {
            throw new EatonParsingException(MISSING_SOL_OR_EOL);
        }
    }

    private static void throwExceptionWhenInvalidHexValues(final String[] messageParts) {
        for (String s : messageParts) {
            if (!HEX.matcher(s).matches()) {
                throw new EatonParsingException(NON_HEX_VALUES);
            }
        }
    }
}

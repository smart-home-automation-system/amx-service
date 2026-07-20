package cloud.cholewa.gateway.eaton.utils;

import cloud.cholewa.gateway.infrastructure.error.EatonException;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.apache.commons.lang3.Strings;

import static cloud.cholewa.gateway.eaton.model.Message.EOL;
import static cloud.cholewa.gateway.eaton.model.Message.SOL;
import static cloud.cholewa.gateway.infrastructure.error.ErrorDictionary.INVALID_MESSAGE_LENGTH;
import static cloud.cholewa.gateway.infrastructure.error.ErrorDictionary.MISSING_SOL_OR_EOL;
import static cloud.cholewa.gateway.infrastructure.error.ErrorDictionary.NON_HEX_VALUES;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class MessageValidator {

    public static boolean isValidEatonMessage(final String message) {
        throwExceptionWhenUnknownMessageLength(message);
        throwExceptionWhenInvalidStartOrEndByte(message);
        throwExceptionWhenInvalidHexValues(message);
        return true;
    }

    /*
     * Supported message length are:
     * 1. PayloadType.TX - B1 - 6 bytes
     * 2. PayloadType.RX - C1 - 12 bytes
     * 3. PayloadType.STATUS - C3 - 8 bytes
     * There are an additional two bytes for SOL and EOL
     * */
    private static void throwExceptionWhenUnknownMessageLength(final String message) {
        String[] messageParts = message.split(",");
        if (messageParts.length == 14 || messageParts.length == 10 || messageParts.length == 8) {
            return;
        }
        throw new EatonException(INVALID_MESSAGE_LENGTH);
    }

    /*
     * Eaton message always must starts with SOL(5a) and ends with EOL(a5)
     * */
    private static void throwExceptionWhenInvalidStartOrEndByte(final String message) {
        if (!Strings.CI.startsWith(message, SOL.getValue()) || !Strings.CI.endsWith(message, EOL.getValue())) {
            throw new EatonException(MISSING_SOL_OR_EOL);
        }
    }

    private static void throwExceptionWhenInvalidHexValues(final String message) {
        String[] singleByte = message.split(",");

        for (String s : singleByte) {
            if (!s.matches("[a-fA-F0-9]{1,2}")) {
                throw new EatonException(NON_HEX_VALUES);
            }
        }
    }
}

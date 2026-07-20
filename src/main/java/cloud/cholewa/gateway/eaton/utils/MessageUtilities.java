package cloud.cholewa.gateway.eaton.utils;

import cloud.cholewa.gateway.eaton.model.Message;
import cloud.cholewa.gateway.infrastructure.error.EatonParsingException;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.apache.commons.collections4.CollectionUtils;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static cloud.cholewa.gateway.infrastructure.error.ErrorDictionary.DATA_POINT_INVALID;
import static cloud.cholewa.gateway.infrastructure.error.ErrorDictionary.EXTRACTING_MESSAGE_ERROR;
import static cloud.cholewa.gateway.infrastructure.error.ErrorDictionary.MESSAGE_LENGTH_MISMATCH;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class MessageUtilities {

    /*
     * Every Eaton message starts with SOL and ends with EOL.
     * Every message is validated by MessageUtilities.validateMessage it ensures that the message is valid
     * and starts with SOL and ends with EOL.
     * */
    public static List<String> extractMessage(final String message) {
        List<String> elements = Arrays.stream(message.split(","))
            .filter(element -> !element.isBlank())
            .collect(Collectors.toList());

        if (CollectionUtils.isNotEmpty(elements)) {
            if (elements.get(0).equals(Message.SOL.getValue())) {
                elements.remove(0);
            }
            if (elements.get(elements.size() - 1).equals(Message.EOL.getValue())) {
                elements.remove(elements.size() - 1);
            }
            return elements;
        }

        throw new EatonParsingException(EXTRACTING_MESSAGE_ERROR);
    }

    /*
     * the first byte represents a message length
     * the third byte represents a data point number
     * */
    public static int extractDataPoint(final List<String> elements) {
        throwExceptionWhenMessageLengthInvalid(elements);

        try {
            if (Integer.parseInt(elements.get(2), 16) < 1 || Integer.parseInt(elements.get(2), 16) > 99) {
                throw new EatonParsingException(DATA_POINT_INVALID);
            }
            return Integer.parseInt(elements.get(2), 16);
        } catch (NumberFormatException e) {
            throw new EatonParsingException(DATA_POINT_INVALID);
        }
    }

    /*
     * the first byte represents a message length, and a message needs to be exactly this length
     * */
    private static void throwExceptionWhenMessageLengthInvalid(final List<String> message) {
        if (Integer.parseInt(message.get(0), 16) != message.size()) {
            throw new EatonParsingException(MESSAGE_LENGTH_MISMATCH);
        }
    }
}

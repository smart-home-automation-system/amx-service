package cloud.cholewa.gateway.infrastructure.error;

public class EatonParsingException extends RuntimeException {

    public EatonParsingException(final ErrorDictionary message) {
        super(message.getMessage());
    }
}

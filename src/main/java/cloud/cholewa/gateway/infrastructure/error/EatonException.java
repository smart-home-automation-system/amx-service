package cloud.cholewa.gateway.infrastructure.error;

public class EatonException extends RuntimeException {

    public EatonException(final ErrorDictionary message) {
        super(message.getMessage());
    }
}

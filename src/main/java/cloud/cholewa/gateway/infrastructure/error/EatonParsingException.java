package cloud.cholewa.gateway.infrastructure.error;

public class EatonParsingException extends RuntimeException {

    public EatonParsingException(final CustomError message) {
        super(message.getDescription());
    }
}

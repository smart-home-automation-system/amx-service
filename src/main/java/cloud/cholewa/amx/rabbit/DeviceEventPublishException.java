package cloud.cholewa.amx.rabbit;

//an event the broker did not take: not sent, refused, not confirmed in time or routed to no queue
public class DeviceEventPublishException extends RuntimeException {

    public DeviceEventPublishException(final String message) {
        super(message);
    }

    public DeviceEventPublishException(final String message, final Throwable cause) {
        super(message, cause);
    }
}

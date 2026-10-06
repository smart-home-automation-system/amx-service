package cloud.cholewa.amx.infrastructure.error;

import cloud.cholewa.commons.error.model.ErrorMessage;
import lombok.Getter;
import org.springframework.http.HttpStatus;

import java.util.Set;

//errorMessages - what the caller is answered with; downstreamMessages - what database-service said
//and only the log may repeat: the details of a failing service are raw exception text (SQL included),
//and POST /home/amx is reachable from outside
@Getter
public class ConfigurationCallException extends RuntimeException {

    private final HttpStatus httpStatus;
    private final transient Set<ErrorMessage> errorMessages;
    private final transient Set<ErrorMessage> downstreamMessages;

    public ConfigurationCallException(final HttpStatus httpStatus, final Set<ErrorMessage> errorMessages) {
        this(httpStatus, errorMessages, Set.of());
    }

    public ConfigurationCallException(
        final HttpStatus httpStatus,
        final Set<ErrorMessage> errorMessages,
        final Set<ErrorMessage> downstreamMessages
    ) {
        this.httpStatus = httpStatus;
        this.errorMessages = errorMessages;
        this.downstreamMessages = downstreamMessages;
    }
}

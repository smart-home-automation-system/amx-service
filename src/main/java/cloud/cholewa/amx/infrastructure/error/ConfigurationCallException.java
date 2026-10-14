package cloud.cholewa.amx.infrastructure.error;

import cloud.cholewa.commons.error.model.ErrorMessage;
import lombok.Getter;
import org.springframework.http.HttpStatus;

import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

//errorMessages - the cause as this service names it, answered to a caller of a command;
//downstreamMessages - what database-service said about a failure, raw exception text (SQL
//included) that belongs in the log line alone
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
        this.errorMessages = errorMessages == null ? Set.of() : errorMessages;
        this.downstreamMessages = downstreamMessages == null ? Set.of() : downstreamMessages;
    }

    //the cause in one line - what a caller is answered as details
    public String cause() {
        return join(errorMessages);
    }

    //the line of the log: the status database-service was resolved to, the cause, and after
    //"- downstream:" what database-service itself said
    public String describe() {
        final String status = httpStatus == null ? "no status" : String.valueOf(httpStatus.value());
        return downstreamMessages.isEmpty()
            ? "lookup failed (" + status + "): " + cause()
            : "lookup failed (" + status + "): " + cause() + " - downstream: " + join(downstreamMessages);
    }

    private static String join(final Set<ErrorMessage> messages) {
        return messages.stream()
            //a message without details (e.g. {"message":"boom"}) must not render as the string
            //"null", and one without either must not throw
            .map(message -> message.getDetails() != null ? message.getDetails() : message.getMessage())
            .filter(Objects::nonNull)
            .collect(Collectors.joining(", "));
    }
}

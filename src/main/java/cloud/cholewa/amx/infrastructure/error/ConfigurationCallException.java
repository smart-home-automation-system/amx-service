package cloud.cholewa.amx.infrastructure.error;

import cloud.cholewa.commons.error.model.ErrorMessage;
import lombok.Getter;
import org.springframework.http.HttpStatus;

import java.util.Set;

@Getter
public class ConfigurationCallException extends RuntimeException {

    private final HttpStatus httpStatus;
    private final transient Set<ErrorMessage> errorMessages;

    public ConfigurationCallException(final HttpStatus httpStatus, final Set<ErrorMessage> errorMessages) {
        this.httpStatus = httpStatus;
        this.errorMessages = errorMessages;
    }
}

package cloud.cholewa.amx.infrastructure.error.processor;

import cloud.cholewa.commons.error.model.ErrorMessage;
import cloud.cholewa.commons.error.model.Errors;
import cloud.cholewa.commons.error.processor.ExceptionProcessor;
import cloud.cholewa.amx.infrastructure.error.ConfigurationCallException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;

import java.util.Collections;
import java.util.Objects;
import java.util.stream.Collectors;

//answers with the status DeviceDatabaseClient resolved - 404 for an unknown data point, 502 for a
//failing or unreachable database-service, 504 for one that did not answer in time - instead of a
//blanket 400 that told the AMX controller its own request was wrong
@Slf4j
public class ConfigurationCallExceptionProcessor implements ExceptionProcessor {

    @Override
    public Errors apply(final Throwable throwable) {
        ConfigurationCallException exception = (ConfigurationCallException) throwable;

        final HttpStatus status = Objects.requireNonNullElse(exception.getHttpStatus(), HttpStatus.BAD_GATEWAY);

        String errorMessages = exception.getErrorMessages().stream()
            //a message without details (e.g. {"message":"boom"}) must not render as the string "null"
            .map(message -> Objects.requireNonNullElse(message.getDetails(), message.getMessage()))
            .filter(Objects::nonNull)
            .collect(Collectors.joining(", "));

        if (status.is5xxServerError()) {
            log.error("Error processing configuration call ({}): {}", status.value(), errorMessages);
        } else {
            log.warn("Error processing configuration call ({}): {}", status.value(), errorMessages);
        }

        return Errors.builder()
            .httpStatus(status)
            .errors(Collections.singleton(
                ErrorMessage.builder()
                    .message("Error processing configuration call")
                    .details(errorMessages)
                    .build()
            ))
            .build();
    }
}

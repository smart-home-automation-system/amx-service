package cloud.cholewa.amx.infrastructure.error.processor;

import cloud.cholewa.amx.infrastructure.error.ConfigurationCallException;
import cloud.cholewa.commons.error.model.ErrorMessage;
import cloud.cholewa.commons.error.model.Errors;
import cloud.cholewa.commons.error.processor.ExceptionProcessor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;

import java.util.Collections;
import java.util.Objects;

//answers a command whose registry call failed with the status the client resolved - 502 for a
//failing or unreachable database-service, 504 for one that did not answer in time - instead of
//a blanket 400 that told the caller its own request was wrong. What database-service said about
//the failure stays in the log line: it is not part of the answer
@Slf4j
public class ConfigurationCallExceptionProcessor implements ExceptionProcessor {

    @Override
    public Errors apply(final Throwable throwable) {
        ConfigurationCallException exception = (ConfigurationCallException) throwable;

        final HttpStatus status = Objects.requireNonNullElse(exception.getHttpStatus(), HttpStatus.BAD_GATEWAY);

        if (status.is5xxServerError()) {
            log.error("Error processing configuration call: {}", exception.describe());
        } else {
            log.warn("Error processing configuration call: {}", exception.describe());
        }

        return Errors.builder()
            .httpStatus(status)
            .errors(Collections.singleton(
                ErrorMessage.builder()
                    .message("Error processing configuration call")
                    .details(exception.cause())
                    .build()
            ))
            .build();
    }
}

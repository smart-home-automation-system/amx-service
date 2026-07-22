package cloud.cholewa.amx.infrastructure.error.processor;

import cloud.cholewa.commons.error.model.ErrorMessage;
import cloud.cholewa.commons.error.model.Errors;
import cloud.cholewa.commons.error.processor.ExceptionProcessor;
import cloud.cholewa.amx.infrastructure.error.ConfigurationCallException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;

import java.util.Collections;
import java.util.stream.Collectors;

@Slf4j
public class ConfigurationCallExceptionProcessor implements ExceptionProcessor {

    @Override
    public Errors apply(final Throwable throwable) {
        ConfigurationCallException exception = (ConfigurationCallException) throwable;

        String errorMessages = exception.getErrorMessages().stream()
            .map(ErrorMessage::getDetails)
            .collect(Collectors.joining(", "));

        log.error("Error processing configuration call: {}", errorMessages);

        return Errors.builder()
            .httpStatus(HttpStatus.BAD_REQUEST)
            .errors(Collections.singleton(
                ErrorMessage.builder()
                    .message("Error processing configuration call")
                    .details(errorMessages)
                    .build()
            ))
            .build();
    }
}

package cloud.cholewa.amx.infrastructure.error.processor;

import cloud.cholewa.commons.error.model.ErrorMessage;
import cloud.cholewa.commons.error.model.Errors;
import cloud.cholewa.commons.error.processor.ExceptionProcessor;
import cloud.cholewa.amx.infrastructure.error.HeatingCallException;
import org.springframework.http.HttpStatus;

import java.util.Collections;

public class HeatingCallExceptionProcessor implements ExceptionProcessor {

    @Override
    public Errors apply(final Throwable throwable) {
        HeatingCallException exception = (HeatingCallException) throwable;

        return Errors.builder()
            .httpStatus(HttpStatus.BAD_REQUEST)
            .errors(Collections.singleton(
                    ErrorMessage.builder()
                        .message("Error processing heating-service call")
                        .details(exception.getErrorMessages().stream().findFirst().orElseThrow().getMessage())
                        .build()
                )
            )
            .build();
    }
}

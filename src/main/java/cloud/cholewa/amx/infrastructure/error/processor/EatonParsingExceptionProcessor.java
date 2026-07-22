package cloud.cholewa.amx.infrastructure.error.processor;

import cloud.cholewa.commons.error.model.ErrorMessage;
import cloud.cholewa.commons.error.model.Errors;
import cloud.cholewa.commons.error.processor.ExceptionProcessor;
import cloud.cholewa.amx.infrastructure.error.CustomError;
import org.springframework.http.HttpStatus;

import java.util.Collections;

public class EatonParsingExceptionProcessor implements ExceptionProcessor {

    @Override
    public Errors apply(final Throwable throwable) {
        return Errors.builder()
            .httpStatus(HttpStatus.BAD_REQUEST)
            .errors(Collections.singleton(
                ErrorMessage.builder()
                    .message(CustomError.EATON_PARSING.getDescription())
                    .details(throwable.getMessage())
                    .build()
            ))
            .build();
    }
}

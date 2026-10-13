package cloud.cholewa.amx.infrastructure.error.processor;

import cloud.cholewa.amx.infrastructure.error.EatonCommandException;
import cloud.cholewa.commons.error.model.ErrorId;
import cloud.cholewa.commons.error.model.ErrorMessage;
import cloud.cholewa.commons.error.model.Errors;
import cloud.cholewa.commons.error.processor.ExceptionProcessor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;

import java.util.Collections;

//answers a failed command with the status and the code of its cause. A 5xx - the frame server
//away, silent or refusing - is logged at ERROR, where the alerts look; a 4xx is the caller's
//(a point without a device) and a WARN
@Slf4j
public class EatonCommandExceptionProcessor implements ExceptionProcessor {

    @Override
    public Errors apply(final Throwable throwable) {
        final EatonCommandException exception = (EatonCommandException) throwable;

        final HttpStatus status = exception.getHttpStatus();
        final String code = ErrorId.codeOf(exception.getError());

        if (status.is5xxServerError()) {
            log.error("Eaton command failed ({}, {}): {}", status.value(), code, exception.getMessage());
        } else {
            log.warn("Eaton command refused ({}, {}): {}", status.value(), code, exception.getMessage());
        }

        return Errors.builder()
            .httpStatus(status)
            .errors(Collections.singleton(
                ErrorMessage.builder()
                    .message(exception.getError().getDescription())
                    .details(exception.getMessage())
                    .code(code)
                    .build()
            ))
            .build();
    }
}

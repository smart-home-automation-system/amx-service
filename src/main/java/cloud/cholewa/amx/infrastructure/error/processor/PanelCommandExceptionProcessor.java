package cloud.cholewa.amx.infrastructure.error.processor;

import cloud.cholewa.amx.infrastructure.error.PanelCommandException;
import cloud.cholewa.commons.error.model.ErrorId;
import cloud.cholewa.commons.error.model.ErrorMessage;
import cloud.cholewa.commons.error.model.Errors;
import cloud.cholewa.commons.error.processor.ExceptionProcessor;
import lombok.extern.slf4j.Slf4j;

import java.util.Collections;

//answers a failed command of the panel with the status and the code of its cause, logged at ERROR:
//every cause is on the side of the panel server - away, silent or refusing
@Slf4j
public class PanelCommandExceptionProcessor implements ExceptionProcessor {

    @Override
    public Errors apply(final Throwable throwable) {
        final PanelCommandException exception = (PanelCommandException) throwable;
        final String code = ErrorId.codeOf(exception.getError());

        log.error("Panel command failed ({}, {}): {}", exception.getHttpStatus().value(), code, exception.getMessage());

        return Errors.builder()
            .httpStatus(exception.getHttpStatus())
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

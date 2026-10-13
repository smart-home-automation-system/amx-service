package cloud.cholewa.amx.infrastructure.error.processor;

import cloud.cholewa.commons.error.model.ErrorMessage;
import cloud.cholewa.commons.error.model.Errors;
import cloud.cholewa.commons.error.processor.ExceptionProcessor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.MessageSourceResolvable;
import org.springframework.http.HttpStatus;
import org.springframework.web.method.annotation.HandlerMethodValidationException;

import java.util.Collections;
import java.util.Objects;
import java.util.stream.Collectors;

//A constraint on a query parameter or path variable that was not met. WebFlux validates those
//itself and raises HandlerMethodValidationException, which the cholewa-commons default would
//answer as a bare "Validation failure" - a 400 that does not say what was wrong. Here the body
//carries the message of each violated constraint. Those messages are written on the constraints
//(message = ...), so the answer does not follow the locale of the JVM and names no Java method.
//A copy of the processor of database-service - the second one, so it stays in the services
@Slf4j
public class InvalidRequestParameterProcessor implements ExceptionProcessor {

    private static final String MESSAGE = "Invalid request parameter";

    @Override
    public Errors apply(final Throwable throwable) {
        final HandlerMethodValidationException exception = (HandlerMethodValidationException) throwable;

        final String violations = exception.getParameterValidationResults().stream()
            .flatMap(result -> result.getResolvableErrors().stream())
            .map(MessageSourceResolvable::getDefaultMessage)
            .filter(Objects::nonNull)
            .distinct()
            .sorted()
            .collect(Collectors.joining("; "));

        log.warn("Handled [{}]: {}", throwable.getClass().getSimpleName(), violations);

        return Errors.builder()
            .httpStatus(HttpStatus.BAD_REQUEST)
            .errors(Collections.singleton(
                ErrorMessage.builder()
                    .message(MESSAGE)
                    .details(violations.isEmpty() ? null : violations)
                    .build()
            ))
            .build();
    }
}

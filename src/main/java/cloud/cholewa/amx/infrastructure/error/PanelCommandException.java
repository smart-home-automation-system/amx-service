package cloud.cholewa.amx.infrastructure.error;

import lombok.Getter;
import org.springframework.http.HttpStatus;

//a command for the entrance panel that was not carried out, or may not have been - the shape of
//EatonCommandException: the message is answered as details, so nothing goes into it that a
//response must not carry
@Getter
public class PanelCommandException extends RuntimeException {

    private final HttpStatus httpStatus;
    private final CustomError error;

    public PanelCommandException(final HttpStatus httpStatus, final CustomError error, final String details) {
        super(details);
        this.httpStatus = httpStatus;
        this.error = error;
    }
}

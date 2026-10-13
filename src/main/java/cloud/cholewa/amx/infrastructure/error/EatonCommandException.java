package cloud.cholewa.amx.infrastructure.error;

import lombok.Getter;
import org.springframework.http.HttpStatus;

//a command for an Eaton device that was not carried out, or may not have been: the status says
//whose problem it is, the error names the cause for a caller that has to tell them apart, and
//the message is what the caller is answered with as details - so nothing goes into it that a
//response must not carry
@Getter
public class EatonCommandException extends RuntimeException {

    private final HttpStatus httpStatus;
    private final CustomError error;

    public EatonCommandException(final HttpStatus httpStatus, final CustomError error, final String details) {
        super(details);
        this.httpStatus = httpStatus;
        this.error = error;
    }
}

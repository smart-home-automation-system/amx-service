package cloud.cholewa.amx.eaton.frame;

import lombok.AccessLevel;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

import java.util.Arrays;
import java.util.Optional;

//the data byte of the frame 0E (move) of a blinds actuator, as Eaton_xComfort_Comm sends it
@Getter
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public enum BlindsCommand {

    OPEN("open", 0x01),
    CLOSE("close", 0x00),
    STOP("stop", 0x02);

    private final String parameter;
    private final int data;

    //by the value of the request parameter, exactly as written: Spring would convert an enum
    //parameter by the name of its constant
    public static Optional<BlindsCommand> ofParameter(final String parameter) {
        return Arrays.stream(values())
            .filter(command -> command.parameter.equals(parameter))
            .findFirst();
    }
}

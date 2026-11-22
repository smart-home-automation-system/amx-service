package cloud.cholewa.amx.panelserver;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

import java.util.Arrays;
import java.util.Optional;

/** A line the panel server sends unasked: what happened on the entrance panel. */
@Getter
@RequiredArgsConstructor
public enum PanelLine {

    /** The gate button on the panel was pressed; the opening is the backend's (HAS-487). */
    GATE_OPEN("gate-open");

    private final String line;

    static Optional<PanelLine> of(final String line) {
        return Arrays.stream(values()).filter(known -> known.line.equals(line)).findFirst();
    }
}

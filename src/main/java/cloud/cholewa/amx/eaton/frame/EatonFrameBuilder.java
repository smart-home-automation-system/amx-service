package cloud.cholewa.amx.eaton.frame;

import cloud.cholewa.amx.eaton.model.TxEventType;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;

import java.util.Locale;

/**
 * The frames an Eaton gateway is sent to drive an actuator, as text: eleven bytes in two hex
 * digits each, separated by commas - {@code 5A,09,B1,<point>,<event>,<data>,00,00,00,00,A5}.
 * They are the frames the NetLinx module {@code Eaton_xComfort_Comm_nl3_0_0.axs} writes to the
 * gateway itself; the tests hold them against that source.
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class EatonFrameBuilder {

    public static final int MIN_POINT = 1;
    public static final int MAX_POINT = 99;
    public static final int MAX_LEVEL = 100;

    private static final String FRAME = "5A,09,B1,%02X,%s,%02X,00,00,00,00,A5";

    public static String blinds(final int point, final BlindsCommand command) {
        return frame(point, TxEventType.JALO_MOVE, command.getData());
    }

    public static String lightSwitch(final int point, final boolean on) {
        return frame(point, TxEventType.SWITCH, on ? 0x01 : 0x00);
    }

    //the level of a dimming actuator in percent, 0 to 100 - the byte is the number itself
    public static String lightLevel(final int point, final int level) {
        if (level < 0 || level > MAX_LEVEL) {
            throw new IllegalArgumentException("level must be between 0 and " + MAX_LEVEL);
        }
        return frame(point, TxEventType.PERCENT, level);
    }

    //asks an actuator for its state; it answers with a datagram of its own, which arrives
    //like every other one
    public static String statusRequest(final int point) {
        return frame(point, TxEventType.REQUEST, 0x00);
    }

    private static String frame(final int point, final TxEventType event, final int data) {
        if (point < MIN_POINT || point > MAX_POINT) {
            throw new IllegalArgumentException("point must be between " + MIN_POINT + " and " + MAX_POINT);
        }
        return String.format(Locale.ROOT, FRAME, point, event.getValue(), data);
    }
}

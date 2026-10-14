package cloud.cholewa.amx.eaton.frame;

import cloud.cholewa.amx.eaton.model.TxEventType;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;

import java.util.Locale;

/**
 * The frames an Eaton gateway is sent to drive an actuator, as text: eight bytes in two hex
 * digits each, separated by commas - {@code 5A,06,B1,<point>,<event>,<data>,00,A5}. That is the
 * TX packet of the gateway's protocol as documented (length {@code 06}: itself, the type and
 * four bytes of payload, the last of them the priority), verified on a blind and a light of the
 * house through the frame server on 2026-10-14. The NetLinx module behind the panels still sends
 * the longer form of 2017 ({@code 09} and four bytes of padding), which the gateways accept too.
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class EatonFrameBuilder {

    public static final int MIN_POINT = 1;
    public static final int MAX_POINT = 99;
    public static final int MAX_LEVEL = 100;

    private static final String FRAME = "5A,06,B1,%02X,%s,%02X,00,A5";

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

    //asks an actuator for its state; it answers with a frame of its own, which arrives
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

package cloud.cholewa.gateway.eaton.utils.feature;

import cloud.cholewa.gateway.eaton.model.SignalStrength;
import cloud.cholewa.gateway.infrastructure.error.EatonException;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;

import static cloud.cholewa.gateway.infrastructure.error.ErrorDictionary.SIGNAL_STRENGTH_INVALID;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class SignalStrengthParser {

    public static SignalStrength parseSignalStrength(final String value) {
        int parsedInt;

        try {
            parsedInt = Integer.parseInt(value, 16);
        } catch (NumberFormatException e) {
            throw new EatonException(SIGNAL_STRENGTH_INVALID);
        }

        if (value.length() > 2 || parsedInt > 255) {
            throw new EatonException(SIGNAL_STRENGTH_INVALID);
        }

        if (parsedInt <= 67) {
            return SignalStrength.GOOD;
        } else if (parsedInt <= 75) {
            return SignalStrength.NORMAL;
        } else if (parsedInt <= 90) {
            return SignalStrength.WEAK;
        } else if (parsedInt <= 120) {
            return SignalStrength.VERY_WEAK;
        } else {
            throw new EatonException(SIGNAL_STRENGTH_INVALID);
        }
    }
}

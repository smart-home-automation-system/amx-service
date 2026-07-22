package cloud.cholewa.gateway.eaton.utils.feature;

import cloud.cholewa.gateway.eaton.model.BatteryLevel;
import cloud.cholewa.gateway.infrastructure.error.EatonParsingException;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;

import static cloud.cholewa.gateway.eaton.model.BatteryLevel.EMPTY;
import static cloud.cholewa.gateway.eaton.model.BatteryLevel.GOOD;
import static cloud.cholewa.gateway.eaton.model.BatteryLevel.MAINS_OPERATED;
import static cloud.cholewa.gateway.eaton.model.BatteryLevel.NEW;
import static cloud.cholewa.gateway.eaton.model.BatteryLevel.NOT_AVAILABLE;
import static cloud.cholewa.gateway.eaton.model.BatteryLevel.VERY_WEAK;
import static cloud.cholewa.gateway.eaton.model.BatteryLevel.WEAK;
import static cloud.cholewa.gateway.infrastructure.error.CustomError.BATTERY_LEVEL_INVALID;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class BatteryLevelParser {

    public static BatteryLevel getBatteryLevel(String value) {
        return switch (value) {
            case "0" -> NOT_AVAILABLE;
            case "1" -> EMPTY;
            case "2" -> VERY_WEAK;
            case "3" -> WEAK;
            case "4" -> GOOD;
            case "5" -> NEW;
            case "10" -> MAINS_OPERATED;
            default -> throw new EatonParsingException(BATTERY_LEVEL_INVALID);
        };
    }
}

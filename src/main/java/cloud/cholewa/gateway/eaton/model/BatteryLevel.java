package cloud.cholewa.gateway.eaton.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

public enum BatteryLevel {

    NOT_AVAILABLE("NOT_AVAILABLE"),
    EMPTY("EMPTY"),
    VERY_WEAK("VERY_WEAK"),
    WEAK("WEAK"),
    GOOD("GOOD"),
    NEW("NEW"),
    MAINS_OPERATED("MAINS_OPERATED");

    private String value;

    BatteryLevel(String value) {
        this.value = value;
    }

    @JsonValue
    public String getValue() {
        return value;
    }

    @Override
    public String toString() {
        return String.valueOf(value);
    }

    @JsonCreator
    public static BatteryLevel fromValue(String value) {
        for (BatteryLevel b : BatteryLevel.values()) {
            if (b.value.equals(value)) {
                return b;
            }
        }
        throw new IllegalArgumentException("Unexpected value '" + value + "'");
    }
}

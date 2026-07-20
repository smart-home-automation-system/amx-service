package cloud.cholewa.gateway.eaton.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

public enum TxEventType {

    SWITCH("0A"),
    REQUEST("0B"),
    PERCENT("0C"),
    DIMMING("OD"),
    JALO_MOVE("0E"),
    JALO_STEP("0F"),
    BASIC_MODE("80");

    private String value;

    TxEventType(String value) {
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
    public static TxEventType fromValue(String value) {
        for (TxEventType b : TxEventType.values()) {
            if (b.value.equals(value)) {
                return b;
            }
        }
        throw new IllegalArgumentException("Unexpected value '" + value + "'");
    }
}

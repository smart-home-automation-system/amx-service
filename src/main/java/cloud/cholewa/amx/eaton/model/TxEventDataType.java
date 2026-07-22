package cloud.cholewa.amx.eaton.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

public enum TxEventDataType {

    SWITCH_OFF("00"),
    SWITCH_ON("01"),
    DIMMING_STOP("00"),
    DIMMING_DARKER("04"),
    DIMMING_BRIGHTER("0F"),
    JALO_CLOSE("00"),
    JALO_OPEN("01"),
    REQUEST_DUMMY("00");

    private String value;

    TxEventDataType(String value) {
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
    public static TxEventDataType fromValue(String value) {
        for (TxEventDataType b : TxEventDataType.values()) {
            if (b.value.equals(value)) {
                return b;
            }
        }
        throw new IllegalArgumentException("Unexpected value '" + value + "'");
    }
}

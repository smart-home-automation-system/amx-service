package cloud.cholewa.amx.eaton.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

public enum PayloadType {

    TX("B1"),
    CONFIG("B2"),
    TX_EXT("B3"),
    RX("C1"),
    STATUS("C3"),
    ERROR("C9");

    private String value;

    PayloadType(String value) {
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
    public static PayloadType fromValue(String value) {
        for (PayloadType b : PayloadType.values()) {
            if (b.value.equals(value)) {
                return b;
            }
        }
        throw new IllegalArgumentException("Unexpected value '" + value + "'");
    }
}

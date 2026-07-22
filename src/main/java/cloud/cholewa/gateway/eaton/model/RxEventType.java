package cloud.cholewa.gateway.eaton.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

public enum RxEventType {

    PUSH_BUTTON_ON("50"),
    PUSH_BUTTON_OFF("51"),
    SWITCH_ON("52"),
    SWITCH_OFF("53"),
    UP_PRESSED("54"),
    UP_RELEASED("55"),
    DOWN_PRESSED("56"),
    DOWN_RELEASED("57"),
    ANALOG_VALUE("62"),
    STATUS("70");

    private String value;

    RxEventType(String value) {
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
    public static RxEventType fromValue(String value) {
        for (RxEventType b : RxEventType.values()) {
            if (b.value.equals(value)) {
                return b;
            }
        }
        throw new IllegalArgumentException("Unexpected value '" + value + "'");
    }
}

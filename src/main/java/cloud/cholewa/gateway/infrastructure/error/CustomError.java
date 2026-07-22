package cloud.cholewa.gateway.infrastructure.error;

import cloud.cholewa.commons.error.model.ErrorId;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

@Getter
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public enum CustomError implements ErrorId {

    EATON_PARSING("Invalid Eaton message couldn't be parsed"),
    MISSING_SOL_OR_EOL("Invalid Eaton message - missing SOL or EOL"),
    NON_HEX_VALUES("Invalid Eaton message - contains non hex values"),
    INVALID_MESSAGE_LENGTH("Invalid Eaton message - supported length are 6, 8 and 12"),
    EXTRACTING_MESSAGE_ERROR("Error during extracting message"),
    MESSAGE_LENGTH_MISMATCH("Message length mismatch, message length differ to expected from first byte"),
    DATA_POINT_INVALID("Invalid data point number"),
    TEMPERATURE_SENSOR_INVALID_MESSAGE_LENGTH("Invalid message length for temperature sensor must be 12"),
    TEMPERATURE_SENSOR_INVALID_DEVICE_TYPE("Invalid device type for temperature sensor"),
    SIGNAL_STRENGTH_INVALID("Invalid value for signal strength"),
    BATTERY_LEVEL_INVALID("Invalid value of battery level");

    private final String description;
}

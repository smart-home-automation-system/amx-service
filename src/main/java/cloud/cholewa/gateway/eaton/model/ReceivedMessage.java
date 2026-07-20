package cloud.cholewa.gateway.eaton.model;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import jakarta.annotation.Nullable;
import java.util.Objects;
import lombok.NoArgsConstructor;
import lombok.experimental.SuperBuilder;

@JsonPropertyOrder({ReceivedMessage.JSON_PROPERTY_DEVICE_ID, ReceivedMessage.JSON_PROPERTY_NAME})
@JsonInclude(JsonInclude.Include.NON_EMPTY)
@NoArgsConstructor
@SuperBuilder
public class ReceivedMessage {

    public static final String JSON_PROPERTY_DEVICE_ID = "deviceId";
    private Integer deviceId;
    public static final String JSON_PROPERTY_NAME = "name";
    private String name;

    public ReceivedMessage deviceId(Integer deviceId) {
        this.deviceId = deviceId;
        return this;
    }

    @Nullable
    @JsonProperty(JSON_PROPERTY_DEVICE_ID)
    @JsonInclude(value = JsonInclude.Include.USE_DEFAULTS)
    public Integer getDeviceId() {
        return deviceId;
    }

    @JsonProperty(JSON_PROPERTY_DEVICE_ID)
    @JsonInclude(value = JsonInclude.Include.USE_DEFAULTS)
    public void setDeviceId(Integer deviceId) {
        this.deviceId = deviceId;
    }

    public ReceivedMessage name(String name) {
        this.name = name;
        return this;
    }

    @Nullable
    @JsonProperty(JSON_PROPERTY_NAME)
    @JsonInclude(value = JsonInclude.Include.USE_DEFAULTS)
    public String getName() {
        return name;
    }

    @JsonProperty(JSON_PROPERTY_NAME)
    @JsonInclude(value = JsonInclude.Include.USE_DEFAULTS)
    public void setName(String name) {
        this.name = name;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ReceivedMessage receivedMessage = (ReceivedMessage) o;
        return Objects.equals(this.deviceId, receivedMessage.deviceId)
                && Objects.equals(this.name, receivedMessage.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(deviceId, name);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("class ReceivedMessage {\n");
        sb.append("    deviceId: ").append(toIndentedString(deviceId)).append("\n");
        sb.append("    name: ").append(toIndentedString(name)).append("\n");
        sb.append("}");
        return sb.toString();
    }

    private String toIndentedString(Object o) {
        if (o == null) {
            return "null";
        }
        return o.toString().replace("\n", "\n    ");
    }
}

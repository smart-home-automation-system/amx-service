package cloud.cholewa.amx.api.model;

import java.time.LocalDateTime;

//sentAt - when the frame server of the AMX master took the frame: a local date-time of the house,
//to the second
public record FrameSentResponse(LocalDateTime sentAt) {
}

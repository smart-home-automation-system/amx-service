package cloud.cholewa.amx.api.model;

import java.time.LocalDateTime;

//sentAt - when the panel server of the Office master took the command: a local date-time of the
//house, to the second
public record PanelCommandResponse(LocalDateTime sentAt) {
}

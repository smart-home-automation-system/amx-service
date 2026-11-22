package cloud.cholewa.amx.api;

import cloud.cholewa.amx.api.model.PanelCommandResponse;
import cloud.cholewa.amx.service.PanelCommandService;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Mono;

//The commands of the entrance panel (HAS-484), routed by the /amx/** route of the gateway. 202:
//the master took the command, which says nothing of what the panel showed
@RestController
@RequestMapping("/amx/panel")
@RequiredArgsConstructor
public class PanelController {

    private final PanelCommandService panelCommandService;

    //gate-service rings it with the doorbell of the door station (HAS-485)
    @PostMapping("/doorbell")
    Mono<ResponseEntity<PanelCommandResponse>> ring() {
        return panelCommandService.ring()
            .map(sentAt -> ResponseEntity.accepted().body(new PanelCommandResponse(sentAt)));
    }
}

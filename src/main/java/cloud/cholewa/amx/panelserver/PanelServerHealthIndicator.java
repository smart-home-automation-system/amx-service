package cloud.cholewa.amx.panelserver;

import lombok.RequiredArgsConstructor;
import org.springframework.boot.health.contributor.Health;
import org.springframework.boot.health.contributor.HealthIndicator;
import org.springframework.stereotype.Component;

//whether the socket to the Office master is open, as frameServer shows the one to Television -
//and outside the readiness and liveness groups for the same reason: a master that is off must not
//take the pod out of the Service or restart it
@Component("panelServer")
@RequiredArgsConstructor
public class PanelServerHealthIndicator implements HealthIndicator {

    private final PanelServerClient panelServerClient;

    @Override
    public Health health() {
        final boolean connected = panelServerClient.isConnected();
        return (connected ? Health.up() : Health.down())
            .withDetail("connected", connected)
            .build();
    }
}

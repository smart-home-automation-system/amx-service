package cloud.cholewa.amx.frameserver;

import lombok.RequiredArgsConstructor;
import org.springframework.boot.health.contributor.Health;
import org.springframework.boot.health.contributor.HealthIndicator;
import org.springframework.stereotype.Component;

//whether the socket to the AMX master is open: DOWN without it, because nothing is received and
//no command can be sent. On purpose not part of the readiness or the liveness group (they hold
//the states of the application alone): a master that is switched off would otherwise take the
//pod out of the Service or restart it in a loop, and neither brings the master back
@Component("frameServer")
@RequiredArgsConstructor
public class FrameServerHealthIndicator implements HealthIndicator {

    private final FrameServerClient frameServerClient;

    @Override
    public Health health() {
        final boolean connected = frameServerClient.isConnected();
        return (connected ? Health.up() : Health.down())
            .withDetail("connected", connected)
            .build();
    }
}

package cloud.cholewa.amx.frameserver;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalManagementPort;
import org.springframework.http.HttpStatus;
import org.springframework.http.HttpStatusCode;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.util.function.Tuple2;
import reactor.util.function.Tuples;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

//the test document points the client at a port nothing listens on, so this is the pod whose
//master is switched off: the health says so and names the socket, and the probes do not care -
//neither the Service nor the kubelet can bring the master back, and a pod restarted in a loop
//would not receive the first frame after it returns either
//the broker's indicator is switched off here: there is no broker in a test, and the whole health
//would wait out its connection timeout before answering
@SpringBootTest(
    webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
    properties = {"management.endpoint.health.show-details=always", "management.health.rabbit.enabled=false"}
)
@ActiveProfiles("test")
class FrameServerHealthTest {

    @LocalManagementPort
    private int managementPort;

    @Autowired
    private FrameServerClient frameServerClient;

    @Test
    void should_show_the_socket_down_in_the_health_of_the_application() {
        assertThat(frameServerClient.isConnected()).isFalse();

        final Tuple2<HttpStatusCode, String> health = actuator("/actuator/health");

        assertThat(health.getT1()).isEqualTo(HttpStatus.SERVICE_UNAVAILABLE);
        assertThat(health.getT2())
            .endsWith("\"status\":\"DOWN\"}")
            .contains("\"frameServer\":{\"details\":{\"connected\":false},\"status\":\"DOWN\"}");
    }

    @Test
    void should_keep_the_socket_out_of_the_readiness_and_the_liveness_probes() {
        final Tuple2<HttpStatusCode, String> readiness = actuator("/actuator/health/readiness");
        final Tuple2<HttpStatusCode, String> liveness = actuator("/actuator/health/liveness");

        assertThat(readiness.getT1()).isEqualTo(HttpStatus.OK);
        assertThat(readiness.getT2()).endsWith("\"status\":\"UP\"}").doesNotContain("frameServer");
        assertThat(liveness.getT1()).isEqualTo(HttpStatus.OK);
        assertThat(liveness.getT2()).endsWith("\"status\":\"UP\"}").doesNotContain("frameServer");
    }

    private Tuple2<HttpStatusCode, String> actuator(final String path) {
        return WebClient.create("http://localhost:" + managementPort).get().uri(path)
            .exchangeToMono(response -> response.bodyToMono(String.class)
                .defaultIfEmpty("")
                .map(body -> Tuples.of(response.statusCode(), body)))
            .block(Duration.ofSeconds(10));
    }
}

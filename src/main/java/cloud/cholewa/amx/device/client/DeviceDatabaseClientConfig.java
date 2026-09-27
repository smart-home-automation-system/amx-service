package cloud.cholewa.amx.device.client;

import jakarta.validation.constraints.NotNull;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;
import org.springframework.web.util.UriBuilder;

import java.time.Duration;

@ConfigurationProperties("internal.service.database")
public record DeviceDatabaseClientConfig(
    @NotNull String host,
    @NotNull String port,
    //well below the 30 s after which api-gateway-service answers the AMX controller with a 504
    @DefaultValue("PT5S") Duration responseTimeout
) {
    public UriBuilder getUriBuilder(final UriBuilder uriBuilder) {
        return uriBuilder.scheme("http").host(host).port(port);
    }
}

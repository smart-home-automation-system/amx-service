package cloud.cholewa.amx.device.client;

import cloud.cholewa.commons.error.model.ErrorMessage;
import cloud.cholewa.commons.error.model.Errors;
import cloud.cholewa.amx.infrastructure.error.ConfigurationCallException;
import cloud.cholewa.home.model.EatonConfigurationResponse;
import cloud.cholewa.home.model.EatonGatewayType;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.jspecify.annotations.NonNull;
import org.springframework.http.HttpStatus;
import org.springframework.http.HttpStatusCode;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.ClientResponse;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;

import java.util.Set;
import java.util.concurrent.TimeoutException;

@Slf4j
@Component
@RequiredArgsConstructor
public class DeviceDatabaseClient {

    private final WebClient webClient;
    private final DeviceDatabaseClientConfig config;

    public Mono<EatonConfigurationResponse> getEatonConfiguration(
        final int point,
        final EatonGatewayType gateway
    ) {
        log.info("Querying device configuration on gateway: {} for point {}[{}]", gateway, point, String.format("$%02X", point));

        return webClient
            .get()
            .uri(uriBuilder -> config
                .getUriBuilder(uriBuilder)
                .path("/home/device/configuration/eaton")
                .queryParam("point", point)
                .queryParam("gateway", gateway.name())
                .build()
            )
            .retrieve()
            .onStatus(HttpStatusCode::isError, DeviceDatabaseClient::mapErrorToException)
            .bodyToMono(EatonConfigurationResponse.class)
            //without a bound the call waits for as long as database-service does - on 2026-09-26 that was
            //until the gateway gave up after 30 s, for every datagram, for hours
            .timeout(config.responseTimeout())
            .onErrorMap(TimeoutException.class, e -> new ConfigurationCallException(
                HttpStatus.GATEWAY_TIMEOUT,
                Set.of(ErrorMessage.builder()
                    .message("Configuration call timed out")
                    .details("database-service did not answer within " + config.responseTimeout())
                    .build())
            ));
    }

    private static @NonNull Mono<Throwable> mapErrorToException(final ClientResponse clientResponse) {
        return clientResponse.bodyToMono(Errors.class)
            .flatMap(o -> Mono.error(new ConfigurationCallException(o.getHttpStatus(), o.getErrors())));
    }
}

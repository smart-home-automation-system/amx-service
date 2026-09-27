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
import org.springframework.web.reactive.function.client.WebClientRequestException;
import reactor.core.publisher.Mono;

import java.util.LinkedHashSet;
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
            ))
            //connection refused, DNS failure, reset - database-service is unreachable, not just slow
            .onErrorMap(WebClientRequestException.class, e -> new ConfigurationCallException(
                HttpStatus.BAD_GATEWAY,
                Set.of(ErrorMessage.builder()
                    .message("Configuration call failed")
                    .details("database-service unreachable: " + e.getClass().getSimpleName())
                    .build())
            ));
    }

    //The status comes from the response, not from the body: Errors.httpStatus is @JsonIgnore, so it is
    //always null after decoding. Only a 404 is relayed - database-service knows no device for the data
    //point. Any other error, a 4xx included (a request amx-service built wrongly), is a failure behind
    //amx-service: a bad gateway for its caller, logged at ERROR. The downstream status goes into the details,
    //which are all the log line has when the body is not the Errors contract.
    private static @NonNull Mono<Throwable> mapErrorToException(final ClientResponse clientResponse) {
        final int downstreamStatus = clientResponse.statusCode().value();
        final HttpStatus status = downstreamStatus == HttpStatus.NOT_FOUND.value()
            ? HttpStatus.NOT_FOUND
            : HttpStatus.BAD_GATEWAY;
        final ErrorMessage downstream = ErrorMessage.builder()
            .message("Configuration call failed")
            .details("database-service answered " + downstreamStatus)
            .build();

        return clientResponse.bodyToMono(Errors.class)
            .mapNotNull(Errors::getErrors)
            //a body that is not the Errors contract (e.g. a proxy's HTML page) must not hide the status
            .onErrorResume(e -> {
                log.warn("Unreadable error body from database-service ({}): {}", downstreamStatus, e.getClass().getSimpleName());
                return Mono.empty();
            })
            .defaultIfEmpty(Set.of())
            .<Throwable>map(errors -> {
                final Set<ErrorMessage> messages = new LinkedHashSet<>(errors);
                messages.add(downstream);
                return new ConfigurationCallException(status, messages);
            });
    }
}

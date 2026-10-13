package cloud.cholewa.amx.device.client;

import cloud.cholewa.amx.infrastructure.error.ConfigurationCallException;
import cloud.cholewa.commons.error.DownstreamErrors;
import cloud.cholewa.commons.error.model.DownstreamError;
import cloud.cholewa.commons.error.model.ErrorMessage;
import cloud.cholewa.home.model.DeviceKind;
import cloud.cholewa.home.model.SmartDevice;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.HttpStatus;
import org.springframework.http.HttpStatusCode;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.reactive.function.client.WebClientRequestException;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeoutException;

//The devices of one kind as the registry of database-service holds them. The rules are the ones of
//DeviceDatabaseClient: bounded by the same timeout, every way the call can go wrong ends as a
//ConfigurationCallException, and what a failing database-service says goes to the log, never into
//the answer. Nothing is relayed here - the registry has no "not found" for a list, so any error
//status is a failure behind amx-service
@Slf4j
@Component
@RequiredArgsConstructor
public class DeviceRegistryClient {

    private static final ParameterizedTypeReference<List<SmartDevice>> DEVICES = new ParameterizedTypeReference<>() {
    };

    private final WebClient webClient;
    private final DeviceDatabaseClientConfig config;

    public Mono<List<SmartDevice>> getDevices(final DeviceKind kind) {
        log.info("Querying the device registry for devices of kind: {}", kind);

        return webClient
            .get()
            .uri(uriBuilder -> config
                .getUriBuilder(uriBuilder)
                .path("/home/device/registry")
                .queryParam("kind", kind.getValue())
                .build()
            )
            .retrieve()
            .onStatus(HttpStatusCode::isError, response -> DownstreamErrors.read(response)
                .map(DeviceRegistryClient::mapErrorToException))
            .bodyToMono(DEVICES)
            //a registry without a device of the kind answers an empty list; no body at all is no answer
            .switchIfEmpty(Mono.error(() -> failed("database-service answered without a list of devices")))
            .timeout(config.responseTimeout())
            .onErrorMap(TimeoutException.class, e -> failed(
                HttpStatus.GATEWAY_TIMEOUT,
                "Device registry call timed out",
                "database-service did not answer within " + config.responseTimeout(),
                Set.of()
            ))
            .onErrorMap(WebClientRequestException.class, e ->
                failed("database-service unreachable: " + e.getClass().getSimpleName()))
            //only the type is answered - a decoding error quotes the body - and the exception itself
            //goes to the log here, at WARN: the ERROR line is the processor's
            .onErrorMap(e -> !(e instanceof ConfigurationCallException), e -> {
                log.warn("Unreadable answer of database-service", e);
                return failed("unreadable answer of database-service: " + e.getClass().getSimpleName());
            });
    }

    private static ConfigurationCallException failed(final String details) {
        return failed(HttpStatus.BAD_GATEWAY, "Device registry call failed", details, Set.of());
    }

    private static ConfigurationCallException failed(
        final HttpStatus status,
        final String message,
        final String details,
        final Set<ErrorMessage> downstreamMessages
    ) {
        return new ConfigurationCallException(
            status,
            Set.of(ErrorMessage.builder().message(message).details(details).build()),
            downstreamMessages
        );
    }

    private static Throwable mapErrorToException(final DownstreamError error) {
        return failed(
            HttpStatus.BAD_GATEWAY,
            "Device registry call failed",
            "database-service answered " + error.status().value(),
            error.errors()
        );
    }
}

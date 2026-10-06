package cloud.cholewa.amx.device.client;

import cloud.cholewa.commons.error.DownstreamErrors;
import cloud.cholewa.commons.error.model.DownstreamError;
import cloud.cholewa.commons.error.model.ErrorMessage;
import cloud.cholewa.amx.infrastructure.error.ConfigurationCallException;
import cloud.cholewa.home.model.EatonConfigurationResponse;
import cloud.cholewa.home.model.EatonGatewayType;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.HttpStatusCode;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.reactive.function.client.WebClientRequestException;
import reactor.core.publisher.Mono;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

@Slf4j
@Component
@RequiredArgsConstructor
public class DeviceDatabaseClient {

    //the name of a CustomErrorDescription constant of database-service - wire contract, pinned there
    //by CustomErrorDescriptionTest. Renamed on that side only, every unknown data point turns into a 502
    static final String UNKNOWN_DATA_POINT_CODE = "NOT_FOUND_DEVICE_CONFIGURATION";

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
            .onStatus(HttpStatusCode::isError, response -> DownstreamErrors.read(response)
                .map(DeviceDatabaseClient::mapErrorToException))
            .bodyToMono(EatonConfigurationResponse.class)
            //no body at all (a 2xx, a redirect) or JSON that is no configuration ({} decodes into an
            //object of nulls): without this the datagram is dropped and answered 200, or fails further
            //down as a 500 of amx-service
            .filter(configuration -> configuration.getType() != null && configuration.getRoom() != null)
            .switchIfEmpty(Mono.error(() -> failed("database-service answered without a configuration")))
            //without a bound the call waits for as long as database-service does - on 2026-09-26 that was
            //until the gateway gave up after 30 s, for every datagram, for hours
            .timeout(config.responseTimeout())
            .onErrorMap(TimeoutException.class, e -> failed(
                HttpStatus.GATEWAY_TIMEOUT,
                "Configuration call timed out",
                "database-service did not answer within " + config.responseTimeout(),
                Set.of()
            ))
            //connection refused, DNS failure, reset - database-service is unreachable, not just slow
            .onErrorMap(WebClientRequestException.class, e ->
                failed("database-service unreachable: " + e.getClass().getSimpleName()))
            //everything after the response headers: a connection lost in the middle of the body, a body
            //that does not decode. Only the type is answered - a decoding error quotes the body - and the
            //exception itself goes to the log here, at WARN: the ERROR line is the processor's
            .onErrorMap(e -> !(e instanceof ConfigurationCallException), e -> {
                log.warn("Unreadable answer of database-service", e);
                return failed("unreadable answer of database-service: " + e.getClass().getSimpleName());
            });
    }

    private static ConfigurationCallException failed(final String details) {
        return failed(HttpStatus.BAD_GATEWAY, "Configuration call failed", details, Set.of());
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

    //A 404 is relayed only when database-service names the cause: it knows no device for the data point.
    //A 404 without that code is a path or a route that does not exist (a renamed endpoint, another
    //application answering under the name) - an outage, not an unknown data point. That one and any other
    //error, a 4xx included (a request amx-service built wrongly), is a failure behind amx-service: a bad
    //gateway for its caller, logged at ERROR. Only the named cause is answered with what database-service
    //said about it; everything else it said - the whole body of a failure, any other message next to
    //the named one - goes to the log, and the caller gets the downstream status alone.
    private static Throwable mapErrorToException(final DownstreamError error) {
        final int downstreamStatus = error.status().value();
        final boolean notFound = downstreamStatus == HttpStatus.NOT_FOUND.value();

        if (notFound && error.hasCode(UNKNOWN_DATA_POINT_CODE)) {
            final Map<Boolean, List<ErrorMessage>> byCode = error.errors().stream()
                .collect(Collectors.partitioningBy(message -> UNKNOWN_DATA_POINT_CODE.equals(message.getCode())));
            final Set<ErrorMessage> answered = new LinkedHashSet<>(byCode.get(true));
            answered.add(ErrorMessage.builder()
                .message("Configuration call failed")
                .details("database-service answered 404")
                .build());

            return new ConfigurationCallException(
                HttpStatus.NOT_FOUND, answered, new LinkedHashSet<>(byCode.get(false))
            );
        }
        return failed(
            HttpStatus.BAD_GATEWAY,
            "Configuration call failed",
            notFound
                ? "database-service answered 404 without the code " + UNKNOWN_DATA_POINT_CODE
                : "database-service answered " + downstreamStatus,
            error.errors()
        );
    }
}

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
import java.util.Set;
import java.util.concurrent.TimeoutException;

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

    //A 404 is relayed only when database-service names the cause: it knows no device for the data point.
    //A 404 without that code is a path or a route that does not exist (a renamed endpoint, another
    //application answering under the name) - an outage, not an unknown data point. That one and any other
    //error, a 4xx included (a request amx-service built wrongly), is a failure behind amx-service: a bad
    //gateway for its caller, logged at ERROR. The downstream status goes into the details, which are all
    //the log line has when the body is not the Errors contract.
    private static Throwable mapErrorToException(final DownstreamError error) {
        final int downstreamStatus = error.status().value();
        final boolean notFound = downstreamStatus == HttpStatus.NOT_FOUND.value();
        final boolean unknownDataPoint = notFound && error.hasCode(UNKNOWN_DATA_POINT_CODE);

        final Set<ErrorMessage> messages = new LinkedHashSet<>(error.errors());
        messages.add(ErrorMessage.builder()
            .message("Configuration call failed")
            .details(notFound && !unknownDataPoint
                ? "database-service answered 404 without the code " + UNKNOWN_DATA_POINT_CODE
                : "database-service answered " + downstreamStatus)
            .build());

        return new ConfigurationCallException(
            unknownDataPoint ? HttpStatus.NOT_FOUND : HttpStatus.BAD_GATEWAY,
            messages
        );
    }
}

package cloud.cholewa.amx.config;

import cloud.cholewa.amx.infrastructure.error.ConfigurationCallException;
import cloud.cholewa.amx.infrastructure.error.EatonCommandException;
import cloud.cholewa.amx.infrastructure.error.processor.ConfigurationCallExceptionProcessor;
import cloud.cholewa.amx.infrastructure.error.processor.EatonCommandExceptionProcessor;
import cloud.cholewa.amx.infrastructure.error.processor.InvalidRequestParameterProcessor;
import cloud.cholewa.commons.error.GlobalErrorExceptionHandler;
import org.springframework.boot.autoconfigure.web.WebProperties;
import org.springframework.boot.webflux.error.ErrorAttributes;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.annotation.Order;
import org.springframework.http.codec.ServerCodecConfigurer;
import org.springframework.web.method.annotation.HandlerMethodValidationException;

import java.util.Map;

//the commands are the only requests this service answers: the frames of the gateways come
//through the socket of the frame server and their failures are log lines of EatonFrameReceiver.
//A failed call to database-service is still answered here - the registry a command asks which
//gateway a point is on fails the same way the configuration lookup of a frame does
@Configuration
public class ExceptionHandlerConfig {

    @Bean
    @Order(-2)
    public GlobalErrorExceptionHandler globalErrorExceptionHandler(
        final ErrorAttributes errorAttributes,
        final WebProperties webProperties,
        final ApplicationContext applicationContext,
        final ServerCodecConfigurer serverCodecConfigurer
    ) {
        GlobalErrorExceptionHandler globalErrorExceptionHandler = new GlobalErrorExceptionHandler(
            errorAttributes, webProperties.getResources(), applicationContext, serverCodecConfigurer
        );

        globalErrorExceptionHandler.withCustomErrorProcessor(
            Map.ofEntries(
                Map.entry(ConfigurationCallException.class, new ConfigurationCallExceptionProcessor()),
                Map.entry(EatonCommandException.class, new EatonCommandExceptionProcessor()),
                Map.entry(HandlerMethodValidationException.class, new InvalidRequestParameterProcessor())
            )
        );

        return globalErrorExceptionHandler;
    }
}

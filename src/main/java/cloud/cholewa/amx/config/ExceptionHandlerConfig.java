package cloud.cholewa.amx.config;

import cloud.cholewa.commons.error.GlobalErrorExceptionHandler;
import cloud.cholewa.amx.infrastructure.error.ConfigurationCallException;
import cloud.cholewa.amx.infrastructure.error.EatonParsingException;
import cloud.cholewa.amx.infrastructure.error.HeatingCallException;
import cloud.cholewa.amx.infrastructure.error.processor.ConfigurationCallExceptionProcessor;
import cloud.cholewa.amx.infrastructure.error.processor.EatonParsingExceptionProcessor;
import cloud.cholewa.amx.infrastructure.error.processor.HeatingCallExceptionProcessor;
import org.springframework.boot.autoconfigure.web.WebProperties;
import org.springframework.boot.webflux.error.ErrorAttributes;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.annotation.Order;
import org.springframework.http.codec.ServerCodecConfigurer;

import java.util.Map;

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
                Map.entry(EatonParsingException.class, new EatonParsingExceptionProcessor()),
                Map.entry(ConfigurationCallException.class, new ConfigurationCallExceptionProcessor()),
                Map.entry(HeatingCallException.class, new HeatingCallExceptionProcessor())
            )
        );

        return globalErrorExceptionHandler;
    }
}

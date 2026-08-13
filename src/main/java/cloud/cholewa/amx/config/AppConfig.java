package cloud.cholewa.amx.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.reactive.function.client.WebClient;

@Configuration
public class AppConfig {

    //the builder is the autoconfigured one (spring-boot-starter-webclient): only a WebClient built
    //from it carries the observation customizer, so an own builder bean would drop the trace
    //on every call to database-service
    @Bean
    WebClient webClient(final WebClient.Builder webClientBuilder) {
        return webClientBuilder.build();
    }
}

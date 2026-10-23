package cloud.cholewa.amx.config;

import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.scheduling.annotation.EnableScheduling;

//The watch of the frame server (HAS-309) runs in the memory of this one instance. Off under the
//test profile: a @SpringBootTest context would otherwise run what the application schedules
@Configuration(proxyBeanMethods = false)
@EnableScheduling
@Profile("!test")
public class SchedulingConfig {
}

package cloud.cholewa.amx;

import cloud.cholewa.amx.device.client.DeviceDatabaseClientConfig;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;

@SpringBootApplication
@EnableConfigurationProperties({DeviceDatabaseClientConfig.class,})
public class AmxServiceApplication {

    public static void main(String[] args) {
        SpringApplication.run(AmxServiceApplication.class, args);
    }

}

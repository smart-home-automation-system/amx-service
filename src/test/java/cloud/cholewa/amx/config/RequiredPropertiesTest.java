package cloud.cholewa.amx.config;

import cloud.cholewa.commons.startup.RequiredPropertiesAutoConfiguration;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.ConfigDataApplicationContextInitializer;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.core.env.StandardEnvironment;

import static org.assertj.core.api.Assertions.assertThat;

//The password of the notifications (HAS-309) has no default in the shared document, and the list of
//cholewa.required-properties makes a pod without it refuse to start (cholewa-commons); the binder alone
//would take the placeholder for the password. This ties the list to the placeholder of application.yaml,
//profile by profile. The active profiles are given here, above the system property surefire sets
class RequiredPropertiesTest {

    //without the variables of the machine: a developer who exported the password for a local run would
    //otherwise see the test of a missing one fail
    private final ApplicationContextRunner contextRunner = new ApplicationContextRunner()
        .withInitializer(context -> context.getEnvironment().getPropertySources()
            .remove(StandardEnvironment.SYSTEM_ENVIRONMENT_PROPERTY_SOURCE_NAME))
        .withInitializer(new ConfigDataApplicationContextInitializer())
        .withConfiguration(AutoConfigurations.of(RequiredPropertiesAutoConfiguration.class));

    @Test
    void should_not_start_in_the_cluster_without_the_password_of_the_notifications() {
        contextRunner
            .withPropertyValues("spring.profiles.active=home")
            .run(context -> assertThat(context.getStartupFailure())
                .hasMessage("A required property has no value: notification.password (set notification-rabbitmq-password)"));
    }

    @Test
    void should_start_in_the_cluster_with_the_password_of_the_notifications() {
        contextRunner
            .withPropertyValues("spring.profiles.active=home", "notification-rabbitmq-password=publisherSecret")
            .run(context -> {
                assertThat(context).hasNotFailed();
                assertThat(context.getEnvironment().getProperty("notification.password")).isEqualTo("publisherSecret");
                assertThat(context.getEnvironment().getProperty("notification.env")).isEqualTo("prod");
            });
    }

    //a run on a workstation publishes nothing unless it is given the real password, and then to dev
    @Test
    void should_start_on_a_workstation_without_the_password() {
        contextRunner
            .withPropertyValues("spring.profiles.active=home,local")
            .run(context -> {
                assertThat(context).hasNotFailed();
                assertThat(context.getEnvironment().getProperty("notification.env")).isEqualTo("dev");
            });
    }

    @Test
    void should_start_under_the_test_profile() {
        contextRunner
            .withPropertyValues("spring.profiles.active=test")
            .run(context -> {
                assertThat(context).hasNotFailed();
                assertThat(context.getEnvironment().getProperty("notification.env")).isEqualTo("test");
            });
    }
}

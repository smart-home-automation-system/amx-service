package cloud.cholewa.amx;

import cloud.cholewa.amx.frameserver.FrameServerWatch;
import cloud.cholewa.commons.notification.NotificationPublisher;
import org.junit.jupiter.api.Test;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.support.converter.JacksonJsonMessageConverter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.ApplicationContext;
import org.springframework.scheduling.annotation.ScheduledAnnotationBeanPostProcessor;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.util.ReflectionTestUtils;

import static org.assertj.core.api.Assertions.assertThat;

//the profile surefire sets does not exist when the class is started from an IDE, and without the
//test document the context has no address of the AMX frame server
@SpringBootTest
@ActiveProfiles("test")
class AmxServiceApplicationTest {

    @Autowired
    private RabbitTemplate rabbitTemplate;

    @Autowired
    private ApplicationContext context;

    @Test
    void contextLoads() {
    }

    //guards the reason RabbitConfig declares no RabbitTemplate: spring.rabbitmq.template.* is
    //applied by RabbitTemplateConfigurer only, and a hand-built template would silently publish
    //without a traceparent header again. There is no getter for the flag, hence the reflection
    @Test
    void should_use_the_auto_configured_template_with_observations_enabled() {
        assertThat(ReflectionTestUtils.getField(rabbitTemplate, "observationEnabled")).isEqualTo(true);
        assertThat(rabbitTemplate.getMessageConverter()).isInstanceOf(JacksonJsonMessageConverter.class);
    }

    //the publisher of the watch of the frame server (HAS-309) goes over its own connection; the one the
    //switch of the library would create uses the connection of the application, which leads to /temperature
    @Test
    void should_publish_the_notifications_over_their_own_connection_only() {
        assertThat(context.getEnvironment().getProperty("cholewa.notification.enabled", Boolean.class, false)).isFalse();
        assertThat(context.getBeanNamesForType(NotificationPublisher.class)).containsExactly("notificationPublisher");
        assertThat(context.getBean(FrameServerWatch.class)).isNotNull();
    }

    //a context of a test must not run what the application schedules
    @Test
    void should_schedule_nothing_under_the_test_profile() {
        assertThat(context.getBeanNamesForType(ScheduledAnnotationBeanPostProcessor.class)).isEmpty();
    }

    //the scheduled watch shares no thread with what a later task schedules (HAS-300, HAS-309)
    @Test
    void should_give_the_scheduled_tasks_a_pool() {
        assertThat(context.getEnvironment().getProperty("spring.task.scheduling.pool.size", Integer.class)).isEqualTo(10);
    }
}

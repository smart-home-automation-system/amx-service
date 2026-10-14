package cloud.cholewa.amx.service;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import cloud.cholewa.amx.frameserver.FrameServerClient;
import cloud.cholewa.amx.frameserver.ReceivedFrame;
import cloud.cholewa.amx.infrastructure.error.ConfigurationCallException;
import cloud.cholewa.amx.infrastructure.error.CustomError;
import cloud.cholewa.amx.infrastructure.error.EatonParsingException;
import cloud.cholewa.commons.error.model.ErrorMessage;
import cloud.cholewa.home.model.EatonGatewayType;
import io.micrometer.observation.Observation;
import io.micrometer.observation.ObservationHandler;
import io.micrometer.observation.ObservationRegistry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.test.StepVerifier;

import java.time.Duration;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

//what becomes of a frame the pipeline cannot take: a line in the log at the level the alerts
//expect, and the next frame is processed all the same
@ExtendWith(MockitoExtension.class)
class EatonFrameReceiverTest {

    private static final ReceivedFrame TEMPERATURE =
        new ReceivedFrame(EatonGatewayType.BLINDS, "5A,0C,C1,2D,62,17,00,00,F0,00,00,3A,10,A5");
    private static final ReceivedFrame ANOTHER =
        new ReceivedFrame(EatonGatewayType.LIGHTS, "5A,0C,C1,2E,62,17,00,00,E2,00,00,41,10,A5");
    private static final Duration PATIENCE = Duration.ofSeconds(10);

    private final ListAppender<ILoggingEvent> receiverLog = new ListAppender<>();
    private final List<Observation.Context> observations = new CopyOnWriteArrayList<>();
    private final ObservationRegistry observationRegistry = ObservationRegistry.create();
    private final Sinks.Many<ReceivedFrame> frames = Sinks.many().unicast().onBackpressureBuffer();

    @Mock
    private FrameServerClient frameServerClient;

    @Mock
    private AmxService amxService;

    private EatonFrameReceiver sut;

    @BeforeEach
    void setUp() {
        receiverLog.start();
        receiverLogger().addAppender(receiverLog);
        observationRegistry.observationConfig().observationHandler(new ObservationHandler<Observation.Context>() {
            @Override
            public boolean supportsContext(final Observation.Context context) {
                return true;
            }

            @Override
            public void onStop(final Observation.Context context) {
                observations.add(context);
            }
        });
        sut = new EatonFrameReceiver(frameServerClient, amxService, observationRegistry);
    }

    @AfterEach
    void tearDown() {
        sut.unsubscribe();
        receiverLogger().detachAppender(receiverLog);
    }

    @Test
    void should_process_every_frame_the_client_hands_over_in_order_and_go_on_after_a_failure() {
        when(frameServerClient.frames()).thenReturn(frames.asFlux());
        when(amxService.consume(TEMPERATURE))
            .thenReturn(Mono.error(new EatonParsingException(CustomError.DATA_POINT_INVALID)));
        when(amxService.consume(ANOTHER)).thenReturn(Mono.empty());

        sut.subscribe();
        frames.tryEmitNext(TEMPERATURE);
        frames.tryEmitNext(ANOTHER);

        await().atMost(PATIENCE).untilAsserted(() -> verify(amxService).consume(ANOTHER));
        assertThat(receiverLog.list).extracting(ILoggingEvent::getLevel)
            .containsExactly(Level.INFO, Level.WARN, Level.INFO);
    }

    @Test
    void should_warn_about_a_frame_the_parser_refuses() {
        when(amxService.consume(any()))
            .thenReturn(Mono.error(new EatonParsingException(CustomError.INVALID_MESSAGE_LENGTH)));

        sut.handle(TEMPERATURE).as(StepVerifier::create).verifyComplete();

        assertThat(receiverLog.list).extracting(ILoggingEvent::getLevel).containsExactly(Level.INFO, Level.WARN);
        assertThat(receiverLog.list.get(1).getFormattedMessage())
            .isEqualTo("Frame [5A,0C,C1,2D,62,17,00,00,F0,00,00,3A,10,A5] from gateway blinds rejected:"
                + " Invalid Eaton message - supported length are 6, 8 and 12");
    }

    //an unknown data point is the registry's business, not an outage
    @Test
    void should_warn_about_a_data_point_database_service_knows_no_device_for() {
        when(amxService.consume(any())).thenReturn(Mono.error(new ConfigurationCallException(
            HttpStatus.NOT_FOUND,
            Set.of(ErrorMessage.builder().message("m").details("Device not found for point: 45 on gateway: blinds").build())
        )));

        sut.handle(TEMPERATURE).as(StepVerifier::create).verifyComplete();

        assertThat(receiverLog.list).extracting(ILoggingEvent::getLevel).containsExactly(Level.INFO, Level.WARN);
        assertThat(receiverLog.list.get(1).getFormattedMessage())
            .isEqualTo("Frame [5A,0C,C1,2D,62,17,00,00,F0,00,00,3A,10,A5] from gateway blinds not processed:"
                + " lookup failed (404): Device not found for point: 45 on gateway: blinds");
    }

    //a failing database-service has to reach the alerts, and they look at ERROR; what it said
    //about the failure is in that line and nowhere else
    @Test
    void should_log_an_error_with_the_words_of_a_failing_database_service() {
        when(amxService.consume(any())).thenReturn(Mono.error(new ConfigurationCallException(
            HttpStatus.BAD_GATEWAY,
            Set.of(ErrorMessage.builder().message("m").details("database-service answered 500").build()),
            Set.of(ErrorMessage.builder().message("Internal error").details("bad SQL grammar").build())
        )));

        sut.handle(TEMPERATURE).as(StepVerifier::create).verifyComplete();

        assertThat(receiverLog.list).extracting(ILoggingEvent::getLevel).containsExactly(Level.INFO, Level.ERROR);
        assertThat(receiverLog.list.get(1).getFormattedMessage())
            .endsWith("not processed: lookup failed (502): database-service answered 500 - downstream: bad SQL grammar");
    }

    @Test
    void should_log_an_error_when_database_service_did_not_answer_in_time() {
        when(amxService.consume(any())).thenReturn(Mono.error(new ConfigurationCallException(
            HttpStatus.GATEWAY_TIMEOUT,
            Set.of(ErrorMessage.builder().message("m").details("database-service did not answer within PT5S").build())
        )));

        sut.handle(TEMPERATURE).as(StepVerifier::create).verifyComplete();

        assertThat(receiverLog.list).extracting(ILoggingEvent::getLevel).containsExactly(Level.INFO, Level.ERROR);
    }

    @Test
    void should_log_an_error_with_the_exception_for_anything_unexpected() {
        when(amxService.consume(any())).thenReturn(Mono.error(new IllegalStateException("the broker is gone")));

        sut.handle(TEMPERATURE).as(StepVerifier::create).verifyComplete();

        assertThat(receiverLog.list).extracting(ILoggingEvent::getLevel).containsExactly(Level.INFO, Level.ERROR);
        assertThat(receiverLog.list.get(1).getThrowableProxy().getMessage()).isEqualTo("the broker is gone");
    }

    //a frame has no request to take a trace from: each one is an observation of its own, started
    //before the pipeline and stopped after it, however it ended
    @Test
    void should_observe_every_frame_on_its_own() {
        when(amxService.consume(TEMPERATURE)).thenReturn(Mono.empty());
        when(amxService.consume(ANOTHER)).thenReturn(Mono.error(new IllegalStateException("boom")));

        sut.handle(TEMPERATURE).as(StepVerifier::create).verifyComplete();
        sut.handle(ANOTHER).as(StepVerifier::create).verifyComplete();

        assertThat(observations).hasSize(2).allSatisfy(context -> {
            assertThat(context.getName()).isEqualTo(EatonFrameReceiver.OBSERVATION);
        });
        assertThat(observations).extracting(context -> context.getLowCardinalityKeyValue("gateway").getValue())
            .containsExactly("blinds", "lights");
    }

    private static Logger receiverLogger() {
        return (Logger) LoggerFactory.getLogger(EatonFrameReceiver.class);
    }
}

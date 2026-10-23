package cloud.cholewa.amx.frameserver;

import cloud.cholewa.commons.notification.NotificationPublisher;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

//HAS-309: the household is told when the AMX master cannot be reached - judged by duration
class FrameServerWatchTest {

    private static final ZoneId ZONE = ZoneId.of("Europe/Warsaw");
    private static final LocalDateTime START = LocalDateTime.of(2026, 10, 22, 17, 10);

    private final FrameServerClient client = mock(FrameServerClient.class);
    private final NotificationPublisher publisher = mock(NotificationPublisher.class);
    private final MovingClock clock = new MovingClock(START.atZone(ZONE).toInstant(), ZONE);
    private FrameServerWatch watch;

    @BeforeEach
    void setUp() {
        when(publisher.publishAlert(anyString())).thenReturn(Mono.empty());
        when(publisher.publishReminder(anyString())).thenReturn(Mono.empty());
        when(publisher.publishInfo(anyString())).thenReturn(Mono.empty());
        watch = new FrameServerWatch(client, publisher, clock);
    }

    @Test
    void should_say_nothing_while_connected() {
        when(client.isConnected()).thenReturn(true);

        pass(1);
        pass(60);

        verifyNoMoreInteractions(publisher);
    }

    @Test
    void should_say_nothing_before_five_minutes() {
        when(client.isConnected()).thenReturn(false);

        pass(1);
        pass(3);
        pass(4);

        verifyNoMoreInteractions(publisher);
    }

    //a master that is off at the start is counted from the start, not from the first pass
    @Test
    void should_alert_once_after_five_minutes_without_a_connection() {
        when(client.isConnected()).thenReturn(false);

        pass(1);
        pass(5);
        pass(6);
        pass(30);

        verify(publisher).publishAlert("Not available: the frame server of the AMX master (Television)"
            + " - not connected since 2026-10-22 17:10 (5 min); no Eaton frame, temperature reading or command gets through.");
        verifyNoMoreInteractions(publisher);
    }

    @Test
    void should_count_a_lost_connection_from_the_first_pass_that_found_it_closed() {
        when(client.isConnected()).thenReturn(true);
        pass(1);
        when(client.isConnected()).thenReturn(false);
        pass(10);
        pass(14);

        verifyNoMoreInteractions(publisher);

        pass(15);

        verify(publisher).publishAlert("Not available: the frame server of the AMX master (Television)"
            + " - not connected since 2026-10-22 17:20 (5 min); no Eaton frame, temperature reading or command gets through.");
    }

    //the night the clock goes back from 03:00 to 02:00 (2026-10-25): five minutes are five minutes,
    //not an hour and five
    @Test
    void should_count_the_time_that_passed_across_the_change_of_the_time() {
        final MovingClock night = new MovingClock(Instant.parse("2026-10-25T00:58:00Z"), ZONE);
        final FrameServerWatch acrossTheChange = new FrameServerWatch(client, publisher, night);
        when(client.isConnected()).thenReturn(false);

        night.at(5);
        acrossTheChange.check().as(StepVerifier::create).verifyComplete();

        verify(publisher).publishAlert("Not available: the frame server of the AMX master (Television)"
            + " - not connected since 2026-10-25 02:58 (5 min); no Eaton frame, temperature reading or command gets through.");
    }

    @Test
    void should_remind_every_hour_while_it_lasts() {
        when(client.isConnected()).thenReturn(false);

        pass(5);
        pass(64);
        pass(65);
        pass(124);
        pass(125);

        verify(publisher).publishAlert(anyString());
        verify(publisher).publishReminder("Still not available: the frame server of the AMX master (Television)"
            + " - not connected since 2026-10-22 17:10 (1 h 5 min).");
        verify(publisher).publishReminder("Still not available: the frame server of the AMX master (Television)"
            + " - not connected since 2026-10-22 17:10 (2 h 5 min).");
        verifyNoMoreInteractions(publisher);
    }

    @Test
    void should_tell_the_return_with_how_long_it_was_out() {
        when(client.isConnected()).thenReturn(false);
        pass(5);
        when(client.isConnected()).thenReturn(true);
        pass(150);
        pass(151);

        verify(publisher).publishAlert(anyString());
        verify(publisher).publishInfo("Available again: the frame server of the AMX master (Television)"
            + " - out from 2026-10-22 17:10 to 2026-10-22 19:40 (2 h 30 min).");
        verifyNoMoreInteractions(publisher);
    }

    //the reconnect after a dropped connection takes a second: no alert, so no return either
    @Test
    void should_say_nothing_of_a_short_break() {
        when(client.isConnected()).thenReturn(true);
        pass(1);
        when(client.isConnected()).thenReturn(false);
        pass(2);
        when(client.isConnected()).thenReturn(true);
        pass(3);
        when(client.isConnected()).thenReturn(false);
        pass(4);
        pass(8);

        verifyNoMoreInteractions(publisher);
    }

    //HAS-109: an alert that never got through is announced with the return
    @Test
    void should_tell_the_return_of_an_outage_whose_alert_did_not_get_through() {
        when(publisher.publishAlert(anyString())).thenReturn(Mono.error(new IllegalStateException("broker down")));
        when(client.isConnected()).thenReturn(false);
        pass(5);
        pass(6);
        when(client.isConnected()).thenReturn(true);
        pass(7);

        verify(publisher, times(2)).publishAlert(anyString());
        verify(publisher).publishInfo("Available again: the frame server of the AMX master (Television)"
            + " - out from 2026-10-22 17:10 to 2026-10-22 17:17 (7 min).");
    }

    @Test
    void should_try_a_failed_alert_again_with_the_next_pass() {
        when(publisher.publishAlert(anyString()))
            .thenReturn(Mono.error(new IllegalStateException("broker down")))
            .thenReturn(Mono.empty());
        when(client.isConnected()).thenReturn(false);

        pass(5);
        pass(6);
        pass(7);

        verify(publisher, times(2)).publishAlert(anyString());
        verify(publisher, never()).publishReminder(anyString());
    }

    @Test
    void should_try_a_failed_return_again_with_the_next_pass() {
        when(publisher.publishInfo(anyString()))
            .thenReturn(Mono.error(new IllegalStateException("broker down")))
            .thenReturn(Mono.empty());
        when(client.isConnected()).thenReturn(false);
        pass(5);
        when(client.isConnected()).thenReturn(true);

        pass(20);
        pass(21);
        pass(22);

        verify(publisher, times(2)).publishInfo(anyString());
    }

    //a notification must neither fail nor stall what it reports on: the watch only looks at the
    //socket, and a broker that does not answer ends the pass after 30 s
    @Test
    void should_end_a_pass_whose_publish_hangs_and_leave_the_client_alone() {
        when(publisher.publishAlert(anyString())).thenReturn(Mono.never());
        when(client.isConnected()).thenReturn(false);
        clock.at(5);

        StepVerifier.withVirtualTime(watch::watch)
            .expectSubscription()
            .expectNoEvent(FrameServerWatch.PUBLISH_TIMEOUT.minusSeconds(1))
            .thenAwait(Duration.ofSeconds(1))
            .verifyComplete();

        verify(client).isConnected();
        verifyNoMoreInteractions(client);
    }

    @Test
    void should_keep_the_publish_bound_below_the_bound_of_the_pass() {
        assertThat(FrameServerWatch.PUBLISH_TIMEOUT).isLessThan(FrameServerWatch.LIMIT);
        assertThat(FrameServerWatch.LIMIT).isLessThan(Duration.ofMinutes(1));
    }

    //Spring calls the method once and subscribes to the same Mono for every run
    @Test
    void should_look_at_the_socket_anew_for_every_run() {
        when(client.isConnected()).thenReturn(false);
        final Mono<Void> scheduled = watch.watch();

        clock.at(1);
        scheduled.as(StepVerifier::create).verifyComplete();
        clock.at(5);
        scheduled.as(StepVerifier::create).verifyComplete();

        verify(client, times(2)).isConnected();
        verify(publisher).publishAlert(anyString());
    }

    private void pass(final int minute) {
        clock.at(minute);
        watch.check().as(StepVerifier::create).verifyComplete();
    }

    //a clock the test moves: minutes after START
    private static final class MovingClock extends Clock {

        private final Instant start;
        private final ZoneId zone;
        private Instant now;

        private MovingClock(final Instant start, final ZoneId zone) {
            this.start = start;
            this.zone = zone;
            this.now = start;
        }

        private void at(final int minute) {
            now = start.plus(Duration.ofMinutes(minute));
        }

        @Override
        public ZoneId getZone() {
            return zone;
        }

        @Override
        public Clock withZone(final ZoneId zone) {
            return this;
        }

        @Override
        public Instant instant() {
            return now;
        }
    }
}

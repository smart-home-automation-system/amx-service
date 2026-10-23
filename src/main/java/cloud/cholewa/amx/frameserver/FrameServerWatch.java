package cloud.cholewa.amx.frameserver;

import cloud.cholewa.commons.notification.NotificationPublisher;
import cloud.cholewa.commons.scheduling.ScheduledPass;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.function.Function;

/**
 * Tells the household when the frame server of the AMX master cannot be reached (HAS-309): on
 * 2026-10-22 it was gone for two and a half hours with one WARN in the log, and no Eaton light,
 * blind or temperature reading reached the backend.
 * <p>
 * Judged by duration, not by single attempts (HAS-109): every minute the socket is looked at, and
 * the time it has been closed counted from the first look that found it closed - from the start of
 * the application while it never connected. Closed for {@link #ALERT_AFTER} is an alert, a
 * reminder every {@link #REMIND_EVERY} while it lasts, and an info when it is open again, with how
 * long it was out. A short break - the reconnect after a dropped connection takes a second - says
 * nothing. A notification that did not get through is tried again with the next pass, and an
 * alert that never got through is still announced by the info on the return.
 * <p>
 * What the watch cannot see: a connection the master dropped without a word looks open until the
 * client takes it for dead after five minutes of silence ({@link FrameServerClient#READ_IDLE_LIMIT}),
 * so such an outage is counted from then. The state is in memory: a restart during an outage
 * counts it again from the start, so its alert may come twice.
 */
@Slf4j
@Component
public class FrameServerWatch {

    static final Duration ALERT_AFTER = Duration.ofMinutes(5);
    static final Duration REMIND_EVERY = Duration.ofHours(1);
    //a notification must neither fail nor stall what it reports on
    static final Duration PUBLISH_TIMEOUT = Duration.ofSeconds(30);
    //below the minute between passes; a pass is a look at the socket and at most one publish
    static final Duration LIMIT = Duration.ofSeconds(50);

    private static final DateTimeFormatter TIME = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm");
    private static final String WHAT = "the frame server of the AMX master (Television)";

    private final FrameServerClient frameServerClient;
    private final NotificationPublisher notificationPublisher;
    private final Clock clock;

    //written by the passes alone, one after the other (fixedDelay); volatile, as a pass may run on
    //another thread of the pool than the one before it
    private volatile Instant closedSince;
    private volatile boolean alertDue;
    private volatile Instant lastNotifiedAt;

    public FrameServerWatch(
        final FrameServerClient frameServerClient,
        final NotificationPublisher notificationPublisher,
        final Clock clock
    ) {
        this.frameServerClient = frameServerClient;
        this.notificationPublisher = notificationPublisher;
        this.clock = clock;
        //not connected until it is: a master that is off at the start is counted from here
        this.closedSince = clock.instant();
    }

    @Scheduled(initialDelayString = "PT1M", fixedDelayString = "PT1M")
    Mono<Void> watch() {
        return ScheduledPass.bounded("AMX frame server watch", LIMIT, this::check);
    }

    Mono<Void> check() {
        final Instant now = clock.instant();
        final Instant since = closedSince;

        if (frameServerClient.isConnected()) {
            if (since == null) {
                return Mono.empty();
            }
            if (!alertDue) {
                closedSince = null;
                return Mono.empty();
            }
            return publish(notificationPublisher::publishInfo, returnText(since, now))
                .doOnNext(sent -> {
                    closedSince = null;
                    alertDue = false;
                    lastNotifiedAt = null;
                })
                .then();
        }

        if (since == null) {
            closedSince = now;
            return Mono.empty();
        }
        if (Duration.between(since, now).compareTo(ALERT_AFTER) < 0) {
            return Mono.empty();
        }
        alertDue = true;
        final Instant notified = lastNotifiedAt;
        if (notified == null) {
            return publish(notificationPublisher::publishAlert, alertText(since, now))
                .doOnNext(sent -> lastNotifiedAt = now)
                .then();
        }
        if (Duration.between(notified, now).compareTo(REMIND_EVERY) >= 0) {
            return publish(notificationPublisher::publishReminder, reminderText(since, now))
                .doOnNext(sent -> lastNotifiedAt = now)
                .then();
        }
        return Mono.empty();
    }

    //Mono.just(true) once the broker took it; empty when it failed (logged, tried again with the next pass)
    private Mono<Boolean> publish(final Function<String, Mono<Void>> publisher, final String text) {
        return Mono.defer(() -> publisher.apply(text))
            .timeout(PUBLISH_TIMEOUT)
            .thenReturn(true)
            .doOnNext(sent -> log.info("Frame server notification published: {}", text))
            .onErrorResume(failure -> {
                log.error("Frame server notification not published, it is tried again with the next pass: {}",
                    failure.toString());
                return Mono.empty();
            });
    }

    String alertText(final Instant since, final Instant now) {
        return "Not available: " + WHAT + " - not connected since " + time(since) + " (" + span(since, now)
            + "); no Eaton frame, temperature reading or command gets through.";
    }

    String reminderText(final Instant since, final Instant now) {
        return "Still not available: " + WHAT + " - not connected since " + time(since) + " ("
            + span(since, now) + ").";
    }

    String returnText(final Instant since, final Instant now) {
        return "Available again: " + WHAT + " - out from " + time(since) + " to " + time(now) + " ("
            + span(since, now) + ").";
    }

    //the time that passed, whatever the clock of the house did meanwhile (the change of the time)
    private static String span(final Instant since, final Instant until) {
        final long total = ChronoUnit.MINUTES.between(since.truncatedTo(ChronoUnit.MINUTES), until.truncatedTo(ChronoUnit.MINUTES));
        final long hours = total / 60;
        final long minutes = total % 60;
        return hours == 0 ? minutes + " min" : hours + " h " + minutes + " min";
    }

    //the clock of the house, for the reader of the message
    private String time(final Instant instant) {
        return TIME.format(LocalDateTime.ofInstant(instant, clock.getZone()));
    }
}

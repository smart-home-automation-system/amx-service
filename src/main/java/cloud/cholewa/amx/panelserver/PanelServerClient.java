package cloud.cholewa.amx.panelserver;

import cloud.cholewa.amx.config.PanelServerProperties;
import cloud.cholewa.amx.infrastructure.error.CustomError;
import cloud.cholewa.amx.infrastructure.error.PanelCommandException;
import cloud.cholewa.amx.lineserver.LineServerClient;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;

import java.time.Clock;
import java.time.Duration;

/**
 * The client of the panel server of the Office master (HAS-484, epic HAS-479): a TCP server in
 * NetLinx that drives the entrance panel at the door - a {@link LineServerClient}, so the
 * connection, the queue of the commands and their answers are those of the frame server. The
 * line protocol is pinned in the {@code CLAUDE.md} of the {@code amx} repository:
 * <ul>
 * <li>to the master: {@code ring} (wake the panel and show the doorbell page), {@code ping} (the
 * keepalive), and with the later tasks of the epic {@code temp;<value>} and {@code gate;ok} /
 * {@code gate;failed};</li>
 * <li>from the master: one answer per line, {@code OK} or {@code ERR unknown command}, and
 * unasked {@code gate-open} - the button on the panel was pressed, never answered.</li>
 * </ul>
 * The panel says nothing on its own for hours, so the connection is asked {@code ping} after
 * {@link #KEEPALIVE_AFTER} without a line from it: a healthy connection is never quiet for the
 * {@link #READ_IDLE_LIMIT} that would replace it, and a dead one leaves the ping unanswered and is
 * replaced within the response timeout. A command while the socket is not open is a 503
 * ({@code PANEL_SERVER_UNAVAILABLE}): the Office master switched off must not touch the Eaton
 * frames, which go through the other socket.
 */
@Slf4j
@Component
public class PanelServerClient extends LineServerClient {

    static final Duration RECONNECT_MIN = Duration.ofSeconds(1);
    static final Duration RECONNECT_MAX = Duration.ofSeconds(30);
    static final Duration READ_IDLE_LIMIT = Duration.ofMinutes(5);
    static final Duration KEEPALIVE_AFTER = Duration.ofMinutes(1);

    static final String RING = "ring";
    static final String PING = "ping";

    private final Object emitting = new Object();
    //a button nobody listens to is no backlog to replay later: a press is now or never
    private final Sinks.Many<PanelLine> lines = Sinks.many().multicast().directBestEffort();

    @Autowired
    public PanelServerClient(final PanelServerProperties properties, final Clock clock) {
        this(properties, clock, RECONNECT_MIN, RECONNECT_MAX, READ_IDLE_LIMIT, KEEPALIVE_AFTER);
    }

    PanelServerClient(
        final PanelServerProperties properties,
        final Clock clock,
        final Duration reconnectMin,
        final Duration reconnectMax,
        final Duration readIdleLimit,
        final Duration keepaliveAfter
    ) {
        super("AMX panel server", properties, clock, reconnectMin, reconnectMax, readIdleLimit,
            new Keepalive(PING, keepaliveAfter));
    }

    /**
     * What happens on the panel, the moment the master tells it, for as long as the client runs.
     * A line that arrives while nobody subscribes is logged and dropped.
     */
    public Flux<PanelLine> panelLines() {
        return lines.asFlux();
    }

    /** Wakes the panel and shows the doorbell page; completes when the master answered {@code OK}. */
    public Mono<Void> ring() {
        return sendLine(RING);
    }

    @Override
    protected boolean unsolicited(final String line) {
        return PanelLine.of(line)
            .map(known -> {
                handOn(known);
                return true;
            })
            .orElse(false);
    }

    @Override
    protected void stopped() {
        synchronized (emitting) {
            lines.tryEmitComplete();
        }
    }

    @Override
    protected RuntimeException notConnected() {
        return unavailable("the AMX panel server is not connected; the command was not sent");
    }

    @Override
    protected RuntimeException lost() {
        return unavailable("the connection to the AMX panel server was lost before its answer;"
            + " the command may have been carried out");
    }

    @Override
    protected RuntimeException shutDown() {
        return unavailable("the panel server client is shut down; the command was not sent");
    }

    @Override
    protected RuntimeException timedOut(final Duration responseTimeout) {
        return new PanelCommandException(
            HttpStatus.GATEWAY_TIMEOUT,
            CustomError.PANEL_SERVER_TIMEOUT,
            "the AMX panel server did not answer within " + responseTimeout + "; the command may have been carried out"
        );
    }

    @Override
    protected RuntimeException refused(final String reason) {
        return new PanelCommandException(
            HttpStatus.BAD_GATEWAY,
            CustomError.PANEL_REFUSED,
            "the AMX panel server refused the command: " + reason
        );
    }

    private static PanelCommandException unavailable(final String details) {
        return new PanelCommandException(HttpStatus.SERVICE_UNAVAILABLE, CustomError.PANEL_SERVER_UNAVAILABLE, details);
    }

    private void handOn(final PanelLine line) {
        final Sinks.EmitResult result;
        synchronized (emitting) {
            result = lines.tryEmitNext(line);
        }
        if (result.isFailure()) {
            log.info("Panel line [{}] dropped: nobody listens ({})", line.getLine(), result);
        } else {
            log.info("Panel line [{}] received", line.getLine());
        }
    }
}

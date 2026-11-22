package cloud.cholewa.amx.frameserver;

import cloud.cholewa.amx.config.FrameServerProperties;
import cloud.cholewa.amx.infrastructure.error.CustomError;
import cloud.cholewa.amx.infrastructure.error.EatonCommandException;
import cloud.cholewa.amx.lineserver.LineServerClient;
import cloud.cholewa.home.model.EatonGatewayType;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.util.concurrent.Queues;

import java.time.Clock;
import java.time.Duration;
import java.time.LocalDateTime;

/**
 * The client of the frame server of the AMX master: a TCP server in NetLinx that moves bytes
 * between this service and the two Eaton gateways on the serial ports, both ways, over one
 * connection - a {@link LineServerClient}, which holds the connection, the queue of the commands
 * and their answers (the line protocol is pinned in the {@code CLAUDE.md} of the {@code amx}
 * repository, next to the NetLinx module):
 * <ul>
 * <li><b>A frame line</b> is {@code <gateway>;<hex>} - {@code blinds} or {@code lights}, then the
 * whole serial frame from {@code 5A} to {@code A5} as comma-separated pairs of hex digits. From
 * the master it is a frame a gateway received, sent the moment it is complete; to the master it
 * is a command: the bytes are written to that gateway.</li>
 * <li><b>An answer</b> is one line per command: {@code OK} once the bytes were handed to the
 * serial device - which says nothing of what the RF telegram did - or {@code ERR <reason>}.</li>
 * </ul>
 * What is received is handed out through {@link #frames()}, in the order it came; a command finds
 * the connection or is refused at once ({@code FRAME_SERVER_UNAVAILABLE}). A frame is never sent
 * twice, and a blind does not move a minute after the request for it was given up.
 * <p>
 * The sensors of the house report every few seconds between them, so a connection on which
 * <b>nothing arrived for {@link #READ_IDLE_LIMIT}</b> is taken for dead, closed and opened again -
 * no keepalive is needed. A command on such a connection before that is a 504. And a frame that
 * arrives while the receiver is more than {@value #FRAME_BUFFER} frames behind is dropped, with a
 * line in the log.
 */
@Slf4j
@Component
public class FrameServerClient extends LineServerClient {

    static final Duration RECONNECT_MIN = Duration.ofSeconds(1);
    static final Duration RECONNECT_MAX = Duration.ofSeconds(30);
    static final Duration READ_IDLE_LIMIT = Duration.ofMinutes(5);
    static final int FRAME_BUFFER = 256;

    private static final String GATEWAY_SEPARATOR = ";";

    private final Object emitting = new Object();
    private final Sinks.Many<ReceivedFrame> frames = Sinks.many().unicast()
        .onBackpressureBuffer(Queues.<ReceivedFrame>get(FRAME_BUFFER).get());

    @Autowired
    public FrameServerClient(final FrameServerProperties properties, final Clock clock) {
        this(properties, clock, RECONNECT_MIN, RECONNECT_MAX, READ_IDLE_LIMIT);
    }

    FrameServerClient(
        final FrameServerProperties properties,
        final Clock clock,
        final Duration reconnectMin,
        final Duration reconnectMax,
        final Duration readIdleLimit
    ) {
        super("AMX frame server", properties, clock, reconnectMin, reconnectMax, readIdleLimit, null);
    }

    /**
     * Every frame the master received from a gateway, in the order it came, for as long as the
     * client runs. One subscriber: the receiver of the application.
     */
    public Flux<ReceivedFrame> frames() {
        return frames.asFlux();
    }

    /**
     * Completes when the server answered {@code OK} - the frame was written to the gateway, which
     * says nothing of what the device did with it. Fails with an {@link EatonCommandException}
     * otherwise.
     */
    public Mono<Void> send(final EatonGatewayType gateway, final String frame) {
        return sendLine(gateway.getValue() + GATEWAY_SEPARATOR + frame);
    }

    @Override
    protected boolean unsolicited(final String line) {
        final int separator = line.indexOf(GATEWAY_SEPARATOR);
        if (separator <= 0) {
            return false;
        }
        final EatonGatewayType gateway = gatewayOf(line.substring(0, separator));
        if (gateway == null) {
            return false;
        }
        frame(gateway, line.substring(separator + 1));
        return true;
    }

    @Override
    protected void stopped() {
        synchronized (emitting) {
            frames.tryEmitComplete();
        }
    }

    @Override
    protected RuntimeException notConnected() {
        return unavailable("the AMX frame server is not connected; the frame was not sent");
    }

    @Override
    protected RuntimeException lost() {
        return unavailable("the connection to the AMX frame server was lost before its answer;"
            + " the frame may have been carried out");
    }

    @Override
    protected RuntimeException shutDown() {
        return unavailable("the frame server client is shut down; the frame was not sent");
    }

    @Override
    protected RuntimeException timedOut(final Duration responseTimeout) {
        return new EatonCommandException(
            HttpStatus.GATEWAY_TIMEOUT,
            CustomError.FRAME_SERVER_TIMEOUT,
            "the AMX frame server did not answer within " + responseTimeout + "; the frame may have been carried out"
        );
    }

    @Override
    protected RuntimeException refused(final String reason) {
        return new EatonCommandException(
            HttpStatus.BAD_GATEWAY,
            CustomError.FRAME_REFUSED,
            "the AMX frame server refused the frame: " + reason
        );
    }

    private static EatonCommandException unavailable(final String details) {
        return new EatonCommandException(HttpStatus.BAD_GATEWAY, CustomError.FRAME_SERVER_UNAVAILABLE, details);
    }

    private static EatonGatewayType gatewayOf(final String name) {
        for (final EatonGatewayType gateway : EatonGatewayType.values()) {
            if (gateway.getValue().equals(name)) {
                return gateway;
            }
        }
        return null;
    }

    //the text is made printable once, here: every line the frame is later logged in is safe
    private void frame(final EatonGatewayType gateway, final String frame) {
        final ReceivedFrame received = new ReceivedFrame(gateway, printable(frame), LocalDateTime.now(clock()));
        final Sinks.EmitResult result;
        synchronized (emitting) {
            result = frames.tryEmitNext(received);
        }
        if (result.isFailure()) {
            log.warn("Frame [{}] of gateway {} dropped: the receiver takes none ({})", received.frame(), gateway, result);
        }
    }
}

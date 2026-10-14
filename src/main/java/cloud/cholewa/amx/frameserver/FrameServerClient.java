package cloud.cholewa.amx.frameserver;

import cloud.cholewa.amx.config.FrameServerProperties;
import cloud.cholewa.amx.infrastructure.error.CustomError;
import cloud.cholewa.amx.infrastructure.error.EatonCommandException;
import cloud.cholewa.home.model.EatonGatewayType;
import io.netty.channel.ChannelOption;
import io.netty.handler.codec.LineBasedFrameDecoder;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.SmartLifecycle;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;
import reactor.core.Disposable;
import reactor.core.Exceptions;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.netty.Connection;
import reactor.netty.tcp.TcpClient;
import reactor.util.concurrent.Queues;
import reactor.util.context.Context;
import reactor.util.retry.Retry;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;

/**
 * The client of the frame server of the AMX master: a TCP server in NetLinx that moves bytes
 * between this service and the two Eaton gateways on the serial ports, both ways, over one
 * connection. Text, one message per line, LF-terminated (the line protocol is pinned in the
 * {@code CLAUDE.md} of the {@code amx} repository, next to the NetLinx module):
 * <ul>
 * <li><b>A frame line</b> is {@code <gateway>;<hex>} - {@code blinds} or {@code lights}, then the
 * whole serial frame from {@code 5A} to {@code A5} as comma-separated pairs of hex digits. From
 * the master it is a frame a gateway received, sent the moment it is complete; to the master it
 * is a command: the bytes are written to that gateway.</li>
 * <li><b>An answer</b> is one line per command, in the order the commands came: {@code OK} once
 * the bytes were handed to the serial device - which says nothing of what the RF telegram did -
 * or {@code ERR <reason>}.</li>
 * <li>A frame line may arrive at any moment, also between a command and its answer; a line that
 * is neither a frame nor an answer is logged and ignored, never taken for an answer; an empty
 * line is ignored.</li>
 * </ul>
 * The connection is permanent: it is the only way to receive, so the client connects when the
 * application starts and connects again, with a backoff, whenever the connection ends - the
 * master takes one client and reopens its port a second after the disconnect. What is received
 * is handed out through {@link #frames()}, in the order it came; a command finds the connection
 * or is refused at once ({@code FRAME_SERVER_UNAVAILABLE}).
 * <p>
 * The answers carry nothing that ties them to a command, so the commands go one after the other,
 * in the order they came: a command is sent only when the one before it was answered or given up.
 * The rules that follow are each held by a test:
 * <ul>
 * <li><b>A frame is never sent twice.</b> Once the line is written, a failure is told to the
 * caller, who knows whether the command is still wanted.</li>
 * <li><b>A command without its answer within {@code response-timeout} ends the connection</b>:
 * its answer may still come, and on a kept connection it would be read as the answer of the next
 * command. The connection is opened again by itself.</li>
 * <li><b>A command its caller no longer waits for is not sent</b>, as long as its line was not
 * written: a blind does not move a minute after the request for it was given up.</li>
 * </ul>
 * What the client cannot know: a connection the other side dropped without a word looks open
 * until a command on it stays unanswered - that one command is a 504 - or until the master
 * closes it properly. And a frame that arrives while the receiver is more than
 * {@value #FRAME_BUFFER} frames behind is dropped, with a line in the log.
 */
@Slf4j
@Component
public class FrameServerClient implements SmartLifecycle {

    static final Duration RECONNECT_MIN = Duration.ofSeconds(1);
    static final Duration RECONNECT_MAX = Duration.ofSeconds(30);
    static final int FRAME_BUFFER = 256;

    //a frame line is at most 255 characters (the longest Eaton frame, C9, is 95)
    private static final int MAX_LINE_LENGTH = 256;
    private static final int MAX_REASON_LENGTH = 100;
    private static final String ACCEPTED = "OK";
    private static final String REFUSED = "ERR";
    private static final String GATEWAY_SEPARATOR = ";";

    private final FrameServerProperties properties;
    private final Duration reconnectMin;
    private final Duration reconnectMax;
    private final TcpClient tcpClient;

    private final Object emitting = new Object();
    private final Sinks.Many<Command> commands = Sinks.many().unicast().onBackpressureBuffer();
    private final Sinks.Many<ReceivedFrame> frames = Sinks.many().unicast()
        .onBackpressureBuffer(Queues.<ReceivedFrame>get(FRAME_BUFFER).get());
    private final Disposable worker;

    //the commands that have no outcome yet: told at the shutdown that they are not carried out
    private final Set<Command> waiting = ConcurrentHashMap.newKeySet();

    private final AtomicReference<Link> link = new AtomicReference<>();
    private final AtomicReference<Disposable> sessions = new AtomicReference<>();
    private final AtomicInteger failedAttempts = new AtomicInteger();
    private volatile boolean running;

    @Autowired
    public FrameServerClient(final FrameServerProperties properties) {
        this(properties, RECONNECT_MIN, RECONNECT_MAX);
    }

    FrameServerClient(final FrameServerProperties properties, final Duration reconnectMin, final Duration reconnectMax) {
        this.properties = properties;
        this.reconnectMin = reconnectMin;
        this.reconnectMax = reconnectMax;
        //no pool: the one connection is kept here
        this.tcpClient = TcpClient.newConnection()
            .host(properties.host())
            .port(properties.port())
            .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, Math.toIntExact(properties.connectTimeout().toMillis()))
            .doOnConnected(connection -> connection.addHandlerLast(new LineBasedFrameDecoder(MAX_LINE_LENGTH)));
        //one command at a time: the next one is started when this one has ended, however it ended
        this.worker = commands.asFlux().concatMap(this::carryOut, 1).subscribe(
            nothing -> { },
            //carryOut lets no failure out; should the queue end all the same, no command is served
            //any more - and that must not be silent
            failure -> log.error("The queue of the AMX frame server client ended: no command is sent any more", failure)
        );
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
        return Mono.deferContextual(context -> {
            final Command command = new Command(gateway.getValue() + GATEWAY_SEPARATOR + frame, Context.of(context));
            waiting.add(command);

            //a sink takes one emission at a time, and requests arrive on several threads. The lock is
            //held for the handing over alone: what the command then does is not waited for here
            final Sinks.EmitResult queued;
            synchronized (emitting) {
                queued = commands.tryEmitNext(command);
            }
            if (queued.isFailure()) {
                waiting.remove(command);
                return Mono.error(shutDownFailure());
            }
            return command.outcome.asMono().doOnCancel(command::abandon);
        });
    }

    //what the health of the application shows
    public boolean isConnected() {
        final Link current = link.get();
        return current != null && current.isOpen();
    }

    @Override
    public void start() {
        if (sessions.get() != null) {
            return;
        }
        running = true;
        sessions.set(session()
            //the connection ended: the next one a moment later, the master reopens its port a second
            //after the disconnect
            .repeatWhen(ended -> ended.delayElements(reconnectMin))
            .subscribe(
                nothing -> { },
                failure -> log.error(
                    "The connection loop of the AMX frame server client ended: nothing is received any more", failure)
            ));
    }

    //a command that is on its way keeps the outcome it has: its line may be written, and "not sent"
    //would be a guess. The connection is closed under it, which ends it as lost
    @Override
    public void stop() {
        running = false;
        //taken first: stopping the worker ends the command that is on its way without an outcome
        final List<Command> left = List.copyOf(waiting);
        worker.dispose();
        final Disposable loop = sessions.getAndSet(null);
        if (loop != null) {
            loop.dispose();
        }
        final Link current = link.getAndSet(null);
        if (current != null) {
            current.close();
        }
        left.forEach(command -> command.outcome.tryEmitError(
            command.isWritten() ? lost() : shutDownFailure()));
        waiting.clear();
        synchronized (emitting) {
            frames.tryEmitComplete();
        }
    }

    @Override
    public boolean isRunning() {
        return running;
    }

    //one connection, kept until it ends; then the Mono completes and the loop opens the next one.
    //Only the connecting is repeated here - with a backoff, for as long as it takes: a master that
    //is switched off is announced once, at WARN, and every further attempt is a DEBUG line
    private Mono<Void> session() {
        return tcpClient.connect()
            .doOnError(failure -> {
                //the address stays in the log: the answer of a command names no host of the home network
                if (failedAttempts.incrementAndGet() == 1) {
                    log.warn(
                        "AMX frame server {}:{} not reached: {}; trying again with a backoff",
                        properties.host(), properties.port(), cause(failure).toString()
                    );
                } else {
                    log.debug(
                        "AMX frame server {}:{} not reached ({} attempts): {}",
                        properties.host(), properties.port(), failedAttempts.get(), cause(failure).toString()
                    );
                }
            })
            .retryWhen(Retry.backoff(Long.MAX_VALUE, reconnectMin).maxBackoff(reconnectMax))
            .flatMap(connection -> {
                final Link opened = new Link(connection, this::received);
                link.set(opened);
                failedAttempts.set(0);
                log.info("Connected to the AMX frame server");
                return opened.ended()
                    .doFinally(signal -> {
                        link.compareAndSet(opened, null);
                        if (running) {
                            log.warn("The connection to the AMX frame server ended; connecting again");
                        }
                    });
            });
    }

    //every line the master sends, told apart as the protocol says: a frame is handed to the
    //receiver, an answer to the command in flight, anything else is logged and ignored
    private void received(final Link from, final String raw) {
        final String line = raw.strip();
        if (line.isEmpty()) {
            return;
        }
        final int separator = line.indexOf(GATEWAY_SEPARATOR);
        if (separator > 0) {
            final EatonGatewayType gateway = gatewayOf(line.substring(0, separator));
            if (gateway != null) {
                frame(gateway, line.substring(separator + 1));
                return;
            }
        }
        if (ACCEPTED.equals(line) || REFUSED.equals(line) || line.startsWith(REFUSED + " ")) {
            if (!from.answer(line)) {
                log.warn("The AMX frame server answered [{}] while no command waited for an answer", printable(line));
            }
            return;
        }
        log.warn("The AMX frame server sent a line that is neither a frame nor an answer, ignored: [{}]", printable(line));
    }

    private static EatonGatewayType gatewayOf(final String name) {
        for (final EatonGatewayType gateway : EatonGatewayType.values()) {
            if (gateway.getValue().equals(name)) {
                return gateway;
            }
        }
        return null;
    }

    private void frame(final EatonGatewayType gateway, final String frame) {
        final Sinks.EmitResult result;
        synchronized (emitting) {
            result = frames.tryEmitNext(new ReceivedFrame(gateway, frame));
        }
        if (result.isFailure()) {
            log.warn("Frame [{}] of gateway {} dropped: the receiver takes none ({})", printable(frame), gateway, result);
        }
    }

    //never fails: what went wrong is the outcome of the command, and the next one still has to run.
    //The context of the request is put back, so the lines logged here carry its traceId
    private Mono<Void> carryOut(final Command command) {
        return Mono.defer(() -> {
                if (command.isAbandoned()) {
                    return skipped(command);
                }
                final Link current = link.get();
                if (current == null || !current.isOpen()) {
                    return Mono.<Void>error(notConnected());
                }
                return exchange(current, command);
            })
            .doOnSuccess(nothing -> command.outcome.tryEmitEmpty())
            .onErrorResume(failure -> {
                command.outcome.tryEmitError(failure);
                return Mono.empty();
            })
            .doFinally(signal -> waiting.remove(command))
            .contextWrite(command.context);
    }

    private static Mono<Void> skipped(final Command command) {
        log.warn("Command [{}] not sent to the AMX frame server: its caller no longer waits", command.line);
        return Mono.empty();
    }

    private Mono<Void> exchange(final Link current, final Command command) {
        command.written.set(true);
        return current.ask(command.line)
            .timeout(properties.responseTimeout())
            .onErrorMap(TimeoutException.class, failure -> new EatonCommandException(
                HttpStatus.GATEWAY_TIMEOUT,
                CustomError.FRAME_SERVER_TIMEOUT,
                "the AMX frame server did not answer within " + properties.responseTimeout()
                    + "; the frame may have been carried out"
            ))
            .onErrorMap(failure -> !(failure instanceof EatonCommandException), failure -> {
                log.warn("Connection to the AMX frame server lost: {}", failure.toString());
                return lost();
            })
            //the answer may still come: on this connection it would be taken for the answer of the
            //next command. The loop opens another
            .doOnError(failure -> current.close())
            .flatMap(reply -> {
                if (ACCEPTED.equals(reply)) {
                    return Mono.empty();
                }
                return Mono.error(new EatonCommandException(
                    HttpStatus.BAD_GATEWAY,
                    CustomError.FRAME_REFUSED,
                    "the AMX frame server refused the frame: " + reason(reply)
                ));
            });
    }

    private static EatonCommandException notConnected() {
        return unavailable("the AMX frame server is not connected; the frame was not sent");
    }

    private static EatonCommandException lost() {
        return unavailable("the connection to the AMX frame server was lost before its answer;"
            + " the frame may have been carried out");
    }

    private static EatonCommandException shutDownFailure() {
        return unavailable("the frame server client is shut down; the frame was not sent");
    }

    private static EatonCommandException unavailable(final String details) {
        return new EatonCommandException(HttpStatus.BAD_GATEWAY, CustomError.FRAME_SERVER_UNAVAILABLE, details);
    }

    //the reason is text of the NetLinx module, answered to the caller: kept short and printable
    private static String reason(final String reply) {
        final String reason = printable(reply.substring(REFUSED.length())).strip();
        if (reason.isEmpty()) {
            return "no reason given";
        }
        return reason.length() > MAX_REASON_LENGTH ? reason.substring(0, MAX_REASON_LENGTH) : reason;
    }

    private static String printable(final String text) {
        return text.replaceAll("[^\\x20-\\x7E]", "?");
    }

    //the retries wrap the failure they gave up on
    private static Throwable cause(final Throwable failure) {
        return failure.getCause() != null && Exceptions.isRetryExhausted(failure)
            ? failure.getCause()
            : failure;
    }

    private static final class Command {

        private final String line;
        private final Context context;
        private final Sinks.Empty<Void> outcome = Sinks.empty();
        private final AtomicBoolean abandoned = new AtomicBoolean();
        private final AtomicBoolean written = new AtomicBoolean();

        private Command(final String line, final Context context) {
            this.line = line;
            this.context = context;
        }

        private void abandon() {
            abandoned.set(true);
        }

        private boolean isAbandoned() {
            return abandoned.get();
        }

        private boolean isWritten() {
            return written.get();
        }
    }

    //the connection, the one answer that is waited for on it and the moment it ends
    private static final class Link {

        private final Connection connection;
        private final AtomicReference<Sinks.One<String>> awaited = new AtomicReference<>();

        private Link(final Connection connection, final BiConsumer<Link, String> lines) {
            this.connection = connection;
            //the decoder in front hands over whole lines; the stream ends when the connection does
            connection.inbound().receive().asString(StandardCharsets.US_ASCII)
                .subscribe(line -> lines.accept(this, line), this::ended, () -> ended(null));
        }

        private boolean isOpen() {
            return !connection.isDisposed() && connection.channel().isActive();
        }

        //whichever side closed it: a connection this client disposes ends its inbound with a
        //cancel, not a completion, so the end is not taken from the stream of lines
        private Mono<Void> ended() {
            return connection.onDispose();
        }

        private void close() {
            connection.dispose();
        }

        private Mono<String> ask(final String line) {
            final Sinks.One<String> reply = Sinks.one();
            awaited.set(reply);
            return connection.outbound().sendString(Mono.just(line + "\n"), StandardCharsets.US_ASCII).then()
                .then(reply.asMono())
                //only its own: by the time this runs the next command may already wait for its answer
                .doFinally(signal -> awaited.compareAndSet(reply, null));
        }

        //whether a command waited for it
        private boolean answer(final String line) {
            final Sinks.One<String> waiting = awaited.get();
            return waiting != null && waiting.tryEmitValue(line).isSuccess();
        }

        private void ended(final Throwable failure) {
            connection.dispose();
            final Sinks.One<String> waiting = awaited.get();
            if (waiting != null) {
                waiting.tryEmitError(failure != null ? failure : new IOException("connection closed by the server"));
            }
        }
    }
}

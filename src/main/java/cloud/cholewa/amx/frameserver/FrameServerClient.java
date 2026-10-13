package cloud.cholewa.amx.frameserver;

import cloud.cholewa.amx.config.FrameServerProperties;
import cloud.cholewa.amx.infrastructure.error.CustomError;
import cloud.cholewa.amx.infrastructure.error.EatonCommandException;
import cloud.cholewa.home.model.EatonGatewayType;
import io.netty.channel.ChannelOption;
import io.netty.handler.codec.LineBasedFrameDecoder;
import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;
import reactor.core.Disposable;
import reactor.core.Exceptions;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.netty.Connection;
import reactor.netty.tcp.TcpClient;
import reactor.util.context.Context;
import reactor.util.retry.Retry;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * The client of the frame server of the AMX master: a TCP server in NetLinx that writes an Eaton
 * frame to the serial gateway it is told. A command is one line, {@code <gateway>;<frame>} ended
 * by LF; the server answers one line, {@code OK} or {@code ERR <text>}.
 * <p>
 * The answers carry nothing that ties them to a command, and the server takes one client at a
 * time. So there is one connection, and the commands go over it one after the other, in the
 * order they came: a command is sent only when the one before it was answered or given up.
 * <p>
 * What the rules below are for:
 * <ul>
 * <li><b>A frame is never sent twice.</b> Only the connecting is repeated, where nothing has
 * left yet. Once the line is written, a failure is told to the caller, who knows whether the
 * command is still wanted.</li>
 * <li><b>Every failure after the line was written ends the connection</b>, the refusal of the
 * server alone left out: the answer of that command may still come, and on a kept connection it
 * would be read as the answer of the next one.</li>
 * <li><b>The connection is closed when nothing was sent for a while</b> and opened again by the
 * next command. Between the cluster and the master a connection that sits idle is forgotten by
 * whatever translates addresses on the way, and the first command on it would be lost to a
 * timeout; it also leaves the server free for the pod that replaces this one in a rollout.</li>
 * <li><b>A command its caller no longer waits for is not sent</b> if it had not been started:
 * a blind does not move a minute after the request for it was given up.</li>
 * </ul>
 */
@Slf4j
@Component
public class FrameServerClient {

    static final int CONNECT_ATTEMPTS = 3;
    static final Duration IDLE_LIMIT = Duration.ofMinutes(2);

    private static final Duration CONNECT_BACKOFF = Duration.ofMillis(200);
    //OK or ERR with a short reason: anything longer is not this protocol
    private static final int MAX_REPLY_LENGTH = 256;
    private static final int MAX_REASON_LENGTH = 100;
    private static final String ACCEPTED = "OK";
    private static final String REFUSED = "ERR";

    private final FrameServerProperties properties;
    private final Duration idleLimit;
    private final TcpClient tcpClient;

    private final Object emitting = new Object();
    private final Sinks.Many<Command> commands = Sinks.many().unicast().onBackpressureBuffer();
    private final Disposable worker;

    private volatile Link link;

    @Autowired
    public FrameServerClient(final FrameServerProperties properties) {
        this(properties, IDLE_LIMIT);
    }

    FrameServerClient(final FrameServerProperties properties, final Duration idleLimit) {
        this.properties = properties;
        this.idleLimit = idleLimit;
        //no pool: the one connection is kept here
        this.tcpClient = TcpClient.newConnection()
            .host(properties.host())
            .port(properties.port())
            .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, Math.toIntExact(properties.connectTimeout().toMillis()))
            .doOnConnected(connection -> connection.addHandlerLast(new LineBasedFrameDecoder(MAX_REPLY_LENGTH)));
        //one command at a time: the next one is started when this one has ended, however it ended
        this.worker = commands.asFlux().concatMap(this::carryOut, 1).subscribe();
    }

    /**
     * Completes when the server answered {@code OK} - the frame was written to the gateway, which
     * says nothing of what the device did with it. Fails with an {@link EatonCommandException}
     * otherwise.
     */
    public Mono<Void> send(final EatonGatewayType gateway, final String frame) {
        return Mono.deferContextual(context -> {
            final Command command = new Command(gateway.getValue() + ";" + frame, Context.of(context));

            //a sink takes one emission at a time, and requests arrive on several threads
            final Sinks.EmitResult queued;
            synchronized (emitting) {
                queued = commands.tryEmitNext(command);
            }
            if (queued.isFailure()) {
                return Mono.error(unavailable("the frame server client is shut down; the frame was not sent"));
            }
            return command.outcome.asMono().doOnCancel(command::abandon);
        });
    }

    boolean hasOpenConnection() {
        final Link current = link;
        return current != null && current.isOpen();
    }

    @PreDestroy
    void shutDown() {
        worker.dispose();
        final Link current = link;
        if (current != null) {
            current.close();
        }
    }

    //never fails: what went wrong is the outcome of the command, and the next one still has to run.
    //The context of the request is put back, so the lines logged here carry its traceId
    private Mono<Void> carryOut(final Command command) {
        return Mono.defer(() -> command.isAbandoned()
                ? skipped(command)
                : connected().flatMap(current -> exchange(current, command.line)))
            .doOnSuccess(nothing -> command.outcome.tryEmitEmpty())
            .onErrorResume(failure -> {
                command.outcome.tryEmitError(failure);
                return Mono.empty();
            })
            .contextWrite(command.context);
    }

    private static Mono<Void> skipped(final Command command) {
        log.warn("Command [{}] not sent to the AMX frame server: its caller no longer waits", command.line);
        return Mono.empty();
    }

    private Mono<Link> connected() {
        final Link current = link;
        if (current != null && current.isOpen()) {
            return Mono.just(current);
        }
        return tcpClient.connect()
            //only the connecting is repeated - nothing was sent yet. A server that still holds the
            //connection of another client (the pod this one replaces) is free a moment later
            .retryWhen(Retry.backoff(CONNECT_ATTEMPTS - 1L, CONNECT_BACKOFF))
            .map(connection -> new Link(connection, idleLimit))
            .doOnNext(opened -> {
                link = opened;
                log.info("Connected to the AMX frame server");
            })
            .onErrorMap(failure -> {
                //the address stays in the log: the answer names no host of the home network
                log.warn(
                    "AMX frame server {}:{} not reached in {} attempts: {}",
                    properties.host(), properties.port(), CONNECT_ATTEMPTS, cause(failure).toString()
                );
                return unavailable("the AMX frame server cannot be reached; the frame was not sent");
            });
    }

    private Mono<Void> exchange(final Link current, final String line) {
        return current.ask(line)
            .timeout(properties.responseTimeout())
            .onErrorMap(TimeoutException.class, failure -> new EatonCommandException(
                HttpStatus.GATEWAY_TIMEOUT,
                CustomError.FRAME_SERVER_TIMEOUT,
                "the AMX frame server did not answer within " + properties.responseTimeout()
                    + "; the frame may have been carried out"
            ))
            .onErrorMap(failure -> !(failure instanceof EatonCommandException), failure -> {
                log.warn("Connection to the AMX frame server lost: {}", failure.toString());
                return unavailable("the connection to the AMX frame server was lost before its answer;"
                    + " the frame may have been carried out");
            })
            //the answer may still come: on this connection it would be taken for the answer of the
            //next command
            .doOnError(failure -> current.close())
            .flatMap(reply -> {
                if (ACCEPTED.equals(reply)) {
                    return Mono.empty();
                }
                if (REFUSED.equals(reply) || reply.startsWith(REFUSED + " ")) {
                    return Mono.error(new EatonCommandException(
                        HttpStatus.BAD_GATEWAY,
                        CustomError.FRAME_REFUSED,
                        "the AMX frame server refused the frame: " + reason(reply)
                    ));
                }
                current.close();
                log.warn("Unreadable answer of the AMX frame server: [{}]", printable(reply));
                return Mono.error(unavailable("the AMX frame server gave an answer that is neither OK nor ERR;"
                    + " the frame may have been carried out"));
            });
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
    }

    //the connection and the one answer that is waited for on it
    private static final class Link {

        private final Connection connection;
        private final AtomicReference<Sinks.One<String>> awaited = new AtomicReference<>();

        private Link(final Connection connection, final Duration idleLimit) {
            this.connection = connection;
            connection.onWriteIdle(idleLimit.toMillis(), () -> {
                log.info("Connection to the AMX frame server closed after {} without a command", idleLimit);
                connection.dispose();
            });
            //the decoder in front hands over whole lines; the stream ends when the connection does
            connection.inbound().receive().asString(StandardCharsets.US_ASCII)
                .subscribe(this::received, this::ended, () -> ended(null));
        }

        private boolean isOpen() {
            return !connection.isDisposed() && connection.channel().isActive();
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

        private void received(final String line) {
            final Sinks.One<String> waiting = awaited.get();
            if (waiting == null || waiting.tryEmitValue(line.strip()).isFailure()) {
                log.warn("The AMX frame server sent a line nobody waited for: [{}]", printable(line));
            }
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

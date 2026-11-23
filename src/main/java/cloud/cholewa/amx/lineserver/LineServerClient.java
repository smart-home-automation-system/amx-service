package cloud.cholewa.amx.lineserver;

import io.netty.channel.ChannelOption;
import io.netty.handler.codec.LineBasedFrameDecoder;
import io.netty.handler.timeout.ReadTimeoutException;
import io.netty.handler.timeout.ReadTimeoutHandler;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.SmartLifecycle;
import reactor.core.Disposable;
import reactor.core.Exceptions;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.netty.Connection;
import reactor.netty.tcp.TcpClient;
import reactor.util.context.Context;
import reactor.util.retry.Retry;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;

/**
 * The client of a line server of an AMX master: a TCP server in NetLinx that takes one client and
 * speaks text, one message per line, LF-terminated (the protocols are pinned in the
 * {@code CLAUDE.md} of the {@code amx} repository, next to the NetLinx modules). Two of them: the
 * frame server of the Television master, which moves the Eaton frames ({@code FrameServerClient}),
 * and the panel server of the Office master, which drives the entrance panel
 * ({@code PanelServerClient}, HAS-484). What they share is here; what a line means is theirs.
 * <ul>
 * <li><b>A command</b> is one line to the master; <b>its answer</b> is one line, in the order the
 * commands came: {@code OK} or {@code ERR <reason>}.</li>
 * <li><b>A line the master sends unasked</b> may arrive at any moment, also between a command and
 * its answer: the subclass tells it apart ({@link #unsolicited}); a line that is neither that nor
 * an answer is logged and ignored, never taken for an answer; an empty line is ignored.</li>
 * </ul>
 * The connection is permanent: the client connects when the application starts and connects
 * again, with a backoff, whenever the connection ends - the master takes one client and reopens
 * its port a second after the disconnect. A command finds the connection or is refused at once.
 * <p>
 * The answers carry nothing that ties them to a command, so the commands go one after the other,
 * in the order they came: a command is sent only when the one before it was answered or given up.
 * The rules that follow are each held by a test:
 * <ul>
 * <li><b>A command is never sent twice.</b> Once the line is written, a failure is told to the
 * caller, who knows whether the command is still wanted.</li>
 * <li><b>A command without its answer within the response timeout ends the connection</b>: its
 * answer may still come, and on a kept connection it would be read as the answer of the next
 * command. The connection is opened again by itself.</li>
 * <li><b>A command its caller no longer waits for is not sent</b>, as long as its line was not
 * written.</li>
 * </ul>
 * A connection the other side dropped without a word looks open to this side, so a connection on
 * which <b>nothing arrived for the read idle limit</b> is taken for dead, closed and opened again.
 * A server that has nothing to say on its own - the panel server - is asked a <b>keepalive</b>
 * line after a quiet while: its answer keeps a healthy connection from ever looking dead, and a
 * dead one gives no answer, which ends it within the response timeout.
 */
@Slf4j
public abstract class LineServerClient implements SmartLifecycle {

    //the socket is stopped after the web server and its graceful shutdown (their phases are
    //DEFAULT_PHASE - 2048 and - 1024): a command served while the pod goes down still finds it
    private static final int PHASE = DEFAULT_PHASE - 4096;

    //a line is at most 255 characters (the longest Eaton frame, C9, is 95)
    private static final int MAX_LINE_LENGTH = 256;
    private static final int MAX_REASON_LENGTH = 100;
    private static final String ACCEPTED = "OK";
    private static final String REFUSED = "ERR";

    private final String name;
    private final LineServerEndpoint endpoint;
    private final Clock clock;
    private final Duration reconnectMin;
    private final Duration reconnectMax;
    private final Duration readIdleLimit;
    private final Keepalive keepalive;
    private final TcpClient tcpClient;

    private final Object emitting = new Object();
    private final Sinks.Many<Command> commands = Sinks.many().unicast().onBackpressureBuffer();
    private final Disposable worker;

    //the commands that have no outcome yet: told at the shutdown that they are not carried out
    private final Set<Command> waiting = ConcurrentHashMap.newKeySet();

    private final AtomicReference<Link> link = new AtomicReference<>();
    private final AtomicReference<Disposable> sessions = new AtomicReference<>();
    private final AtomicReference<Disposable> keepalives = new AtomicReference<>();
    private final AtomicReference<Instant> lastReceivedAt = new AtomicReference<>(Instant.MIN);
    private final AtomicInteger failedAttempts = new AtomicInteger();
    private volatile boolean running;

    /**
     * @param name      what the server is called in the log, e.g. "AMX frame server"
     * @param keepalive the line asked after a quiet while, or {@code null} for a server that talks on its own
     */
    protected LineServerClient(
        final String name,
        final LineServerEndpoint endpoint,
        final Clock clock,
        final Duration reconnectMin,
        final Duration reconnectMax,
        final Duration readIdleLimit,
        final Keepalive keepalive
    ) {
        this.name = name;
        this.endpoint = endpoint;
        this.clock = clock;
        this.reconnectMin = reconnectMin;
        this.reconnectMax = reconnectMax;
        this.readIdleLimit = readIdleLimit;
        this.keepalive = keepalive;
        //no pool: the one connection is kept here
        this.tcpClient = TcpClient.newConnection()
            .host(endpoint.host())
            .port(endpoint.port())
            .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, Math.toIntExact(endpoint.connectTimeout().toMillis()))
            .option(ChannelOption.SO_KEEPALIVE, true)
            .doOnConnected(connection -> connection
                //a connection nothing arrives on is closed by this handler, which ends it like any
                //other failure and the loop opens another
                .addHandlerLast(new ReadTimeoutHandler(readIdleLimit.toMillis(), TimeUnit.MILLISECONDS))
                .addHandlerLast(new LineBasedFrameDecoder(MAX_LINE_LENGTH)));
        //one command at a time: the next one is started when this one has ended, however it ended
        this.worker = commands.asFlux().concatMap(this::carryOut, 1).subscribe(
            nothing -> { },
            //carryOut lets no failure out; should the queue end all the same, no command is served
            //any more - and that must not be silent
            failure -> log.error("The queue of the {} client ended: no command is sent any more", name, failure)
        );
    }

    /**
     * Handles a line the master sent unasked, already stripped and not empty. Answers whether it
     * was one: a line it was not is checked for an answer next.
     */
    protected abstract boolean unsolicited(String line);

    /** No connection, the command not sent. */
    protected abstract RuntimeException notConnected();

    /** The connection ended after the line was written and before its answer. */
    protected abstract RuntimeException lost();

    /** The client is stopped, the command not sent. */
    protected abstract RuntimeException shutDown();

    /** No answer within the response timeout; the command may have been carried out. */
    protected abstract RuntimeException timedOut(Duration responseTimeout);

    /** The server answered {@code ERR}; the reason is short and printable. */
    protected abstract RuntimeException refused(String reason);

    /**
     * Called every time a connection is opened, before any command is carried out on it: a server
     * that forgets what it was told when its client goes away (the panel server, HAS-486) is told it
     * again. Never blocks and never throws: it runs on the thread of the connection.
     */
    protected void connected() {
    }

    /** Called once at the stop, after the connection is closed: the subclass ends what it hands out. */
    protected void stopped() {
    }

    /**
     * Completes when the server answered {@code OK}; fails with what the subclass makes of the
     * failure otherwise.
     */
    protected final Mono<Void> sendLine(final String line) {
        return Mono.deferContextual(context -> {
            final Command command = new Command(line, Context.of(context));
            waiting.add(command);

            //a sink takes one emission at a time, and requests arrive on several threads. The lock is
            //held for the handing over alone: what the command then does is not waited for here
            final Sinks.EmitResult queued;
            synchronized (emitting) {
                queued = commands.tryEmitNext(command);
            }
            if (queued.isFailure()) {
                waiting.remove(command);
                return Mono.error(shutDown());
            }
            return command.outcome.asMono().doOnCancel(command::abandon);
        });
    }

    protected final Clock clock() {
        return clock;
    }

    //the text is made printable once, where it comes in: every line it is later logged in is safe
    protected static String printable(final String text) {
        return text.replaceAll("[^\\x20-\\x7E]", "?");
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
                    "The connection loop of the {} client ended: nothing is received any more", name, failure)
            ));
        if (keepalive != null) {
            keepalives.set(Flux.interval(keepalive.after(), keepalive.after())
                .subscribe(tick -> keepAlive(), failure -> log.error(
                    "The keepalive of the {} client ended: a quiet connection is replaced every {}",
                    name, readIdleLimit, failure)));
        }
    }

    //a command that is on its way keeps the outcome it has: its line may be written, and "not sent"
    //would be a guess. The connection is closed under it, which ends it as lost
    @Override
    public void stop() {
        running = false;
        //taken first: stopping the worker ends the command that is on its way without an outcome
        final List<Command> left = List.copyOf(waiting);
        //taken before the loop is disposed: its cancel clears the link (doFinally of the session),
        //and the connection was left open under a stopped client until HAS-484
        final Link current = link.getAndSet(null);
        worker.dispose();
        disposeOf(keepalives);
        disposeOf(sessions);
        if (current != null) {
            current.close();
        }
        left.forEach(command -> command.outcome.tryEmitError(command.isWritten() ? lost() : shutDown()));
        waiting.clear();
        stopped();
    }

    @Override
    public boolean isRunning() {
        return running;
    }

    @Override
    public int getPhase() {
        return PHASE;
    }

    private static void disposeOf(final AtomicReference<Disposable> held) {
        final Disposable disposable = held.getAndSet(null);
        if (disposable != null) {
            disposable.dispose();
        }
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
                        "{} {}:{} not reached: {}; trying again with a backoff",
                        name, endpoint.host(), endpoint.port(), cause(failure).toString()
                    );
                } else {
                    log.debug(
                        "{} {}:{} not reached ({} attempts): {}",
                        name, endpoint.host(), endpoint.port(), failedAttempts.get(), cause(failure).toString()
                    );
                }
            })
            .retryWhen(Retry.backoff(Long.MAX_VALUE, reconnectMin).maxBackoff(reconnectMax))
            .flatMap(connection -> {
                final Link opened = new Link(connection, readIdleLimit, this::received);
                lastReceivedAt.set(clock.instant());
                link.set(opened);
                failedAttempts.set(0);
                log.info("Connected to the {}", name);
                connected();
                return opened.ended()
                    .doFinally(signal -> {
                        link.compareAndSet(opened, null);
                        if (running) {
                            log.warn("The connection to the {} ended ({}); connecting again", name, opened.whyEnded());
                        }
                    });
            });
    }

    //a connection nothing arrived on for a while is asked the keepalive line, through the queue of
    //the commands - never between a command and its answer. Its outcome is nobody's: a dead
    //connection is closed by the missing answer, and that is logged where the connection ends
    private void keepAlive() {
        if (!isConnected() || clock.instant().isBefore(lastReceivedAt.get().plus(keepalive.after()))) {
            return;
        }
        sendLine(keepalive.line()).subscribe(
            nothing -> { },
            failure -> log.debug("The keepalive of the {} got no answer: {}", name, failure.toString())
        );
    }

    //every line the master sends, told apart as the protocol says: a line it sends unasked is the
    //subclass's, an answer goes to the command in flight, anything else is logged and ignored
    private void received(final Link from, final String raw) {
        lastReceivedAt.set(clock.instant());
        final String line = raw.strip();
        if (line.isEmpty() || unsolicited(line)) {
            return;
        }
        if (ACCEPTED.equals(line) || REFUSED.equals(line) || line.startsWith(REFUSED + " ")) {
            if (!from.answer(line)) {
                log.warn("The {} answered [{}] while no command waited for an answer", name, printable(line));
            }
            return;
        }
        log.warn("The {} sent a line that is neither its own nor an answer, ignored: [{}]", name, printable(line));
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

    private Mono<Void> skipped(final Command command) {
        log.warn("Command [{}] not sent to the {}: its caller no longer waits", command.line, name);
        return Mono.empty();
    }

    private Mono<Void> exchange(final Link current, final Command command) {
        command.written.set(true);
        return current.ask(command.line)
            .timeout(endpoint.responseTimeout())
            .onErrorMap(failure -> {
                if (failure instanceof TimeoutException) {
                    return timedOut(endpoint.responseTimeout());
                }
                log.warn("Connection to the {} lost: {}", name, failure.toString());
                return lost();
            })
            //the answer may still come: on this connection it would be taken for the answer of the
            //next command. The loop opens another
            .doOnError(failure -> current.close())
            .flatMap(reply -> ACCEPTED.equals(reply)
                ? Mono.empty()
                : Mono.error(refused(reason(reply))));
    }

    //the reason is text of the NetLinx module, answered to the caller: kept short and printable
    private static String reason(final String reply) {
        final String reason = printable(reply.substring(REFUSED.length())).strip();
        if (reason.isEmpty()) {
            return "no reason given";
        }
        return reason.length() > MAX_REASON_LENGTH ? reason.substring(0, MAX_REASON_LENGTH) : reason;
    }

    //the retries wrap the failure they gave up on
    private static Throwable cause(final Throwable failure) {
        return failure.getCause() != null && Exceptions.isRetryExhausted(failure)
            ? failure.getCause()
            : failure;
    }

    /** The line asked after {@code after} without a line from the server. */
    public record Keepalive(String line, Duration after) {
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
        private final Duration readIdleLimit;
        private final AtomicReference<Sinks.One<String>> awaited = new AtomicReference<>();
        private volatile String whyEnded = "closed by this side";

        private Link(final Connection connection, final Duration readIdleLimit, final BiConsumer<Link, String> lines) {
            this.connection = connection;
            this.readIdleLimit = readIdleLimit;
            //the decoder in front hands over whole lines; the stream ends when the connection does
            connection.inbound().receive().asString(StandardCharsets.US_ASCII)
                .subscribe(line -> lines.accept(this, line), this::ended, () -> ended(null));
        }

        private String whyEnded() {
            return whyEnded;
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
            if (failure instanceof ReadTimeoutException) {
                whyEnded = "nothing arrived for " + readIdleLimit + ", taken for dead";
            } else if (failure != null) {
                whyEnded = failure.toString();
            } else {
                whyEnded = "closed by the server";
            }
            connection.dispose();
            final Sinks.One<String> waiting = awaited.get();
            if (waiting != null) {
                waiting.tryEmitError(failure != null ? failure : new IOException("connection closed by the server"));
            }
        }
    }
}

package cloud.cholewa.amx.frameserver;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

//What the frame server of the AMX master is to the client: it reads lines and answers one line
//each. Unlike the real one it takes any number of clients, so a test can count the connections
final class FrameServerStub implements AutoCloseable {

    //an answer that is none: the stub closes the connection instead
    static final String HANG_UP = "<hang up>";

    private final ServerSocket serverSocket;
    private final List<Socket> sockets = new CopyOnWriteArrayList<>();
    private final List<String> lines = new CopyOnWriteArrayList<>();
    private final AtomicInteger connections = new AtomicInteger();
    private final AtomicBoolean sentAhead = new AtomicBoolean();

    //null leaves the line without an answer
    private volatile Function<String, String> answers = line -> "OK\n";
    private volatile Duration thinking = Duration.ZERO;

    FrameServerStub() throws IOException {
        serverSocket = new ServerSocket(0, 50, InetAddress.getLoopbackAddress());
        Thread.ofVirtual().start(this::accept);
    }

    int port() {
        return serverSocket.getLocalPort();
    }

    List<String> lines() {
        return lines;
    }

    int connections() {
        return connections.get();
    }

    //whether a line arrived while the one before it had no answer yet
    boolean wasSentAhead() {
        return sentAhead.get();
    }

    void answers(final Function<String, String> answers) {
        this.answers = answers;
    }

    void thinksFor(final Duration thinking) {
        this.thinking = thinking;
    }

    void hangUp() throws IOException {
        for (final Socket socket : sockets) {
            socket.close();
        }
    }

    @Override
    public void close() throws IOException {
        serverSocket.close();
        hangUp();
    }

    private void accept() {
        while (!serverSocket.isClosed()) {
            try {
                final Socket socket = serverSocket.accept();
                connections.incrementAndGet();
                sockets.add(socket);
                Thread.ofVirtual().start(() -> serve(socket));
            } catch (IOException e) {
                return;
            }
        }
    }

    private void serve(final Socket socket) {
        try (socket) {
            final BufferedReader reader = new BufferedReader(
                new InputStreamReader(socket.getInputStream(), StandardCharsets.US_ASCII));
            final OutputStream output = socket.getOutputStream();

            String line;
            while ((line = reader.readLine()) != null) {
                lines.add(line);
                final String answer = answers.apply(line);

                Thread.sleep(thinking);
                if (reader.ready()) {
                    sentAhead.set(true);
                }
                if (HANG_UP.equals(answer)) {
                    return;
                }
                if (answer != null) {
                    output.write(answer.getBytes(StandardCharsets.US_ASCII));
                    output.flush();
                }
            }
        } catch (IOException e) {
            //the client or the test closed the connection
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}

package cloud.cholewa.amx.lineserver;

import java.time.Duration;

/** Where a line server listens and how long it may take: the properties of each server implement it. */
public interface LineServerEndpoint {

    String host();

    Integer port();

    Duration connectTimeout();

    Duration responseTimeout();
}

package cloud.cholewa.amx.device.registry;

import cloud.cholewa.amx.device.client.DeviceRegistryClient;
import cloud.cholewa.amx.infrastructure.error.CustomError;
import cloud.cholewa.amx.infrastructure.error.EatonCommandException;
import cloud.cholewa.home.model.DeviceKind;
import cloud.cholewa.home.model.SmartDevice;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Which Eaton device of a kind sits on a point - the gateway a frame for it has to go to. A
 * command names the point alone, so the device is looked for among the devices of the kind the
 * command is for: a point that holds a room sensor is no blinds, and is not sent a frame.
 * <p>
 * The list of a kind is kept for five minutes, and <b>a kept list is believed only when it says
 * yes</b>: one device on the point. Before a command is refused - no device on the point, one on
 * each gateway, or what {@link #findAgain} is asked for - the registry is read again, so a
 * device registered or corrected a moment ago is driven at once and not five minutes later. The
 * other way round stays: a device that was removed, or moved to the other gateway, is driven the
 * old way until its list is five minutes old.
 * <p>
 * A list read within the last ten seconds is new enough for a refusal too, and callers that ask
 * at the same moment share one read - twenty commands of a schedule are one call to
 * database-service, and so is a client that goes through points nothing is registered on. A
 * failed read is not kept.
 * <p>
 * Whether a device is {@code enabled} is not looked at: that field is the word of the services
 * that decide what to do with a device, and this one only carries what they decided.
 */
@Component
@RequiredArgsConstructor
public class EatonDeviceRegistry {

    static final Duration KEPT_FOR = Duration.ofMinutes(5);
    static final Duration NEW_FOR = Duration.ofSeconds(10);

    private final DeviceRegistryClient deviceRegistryClient;
    private final Clock clock;

    private final Map<DeviceKind, Kept> kept = new ConcurrentHashMap<>();
    private final Map<DeviceKind, Mono<List<SmartDevice>>> reading = new ConcurrentHashMap<>();

    public Mono<SmartDevice> find(final DeviceKind kind, final int point) {
        return Mono.defer(() -> {
            final Kept known = kept.get(kind);
            if (known != null && known.isYoungerThan(KEPT_FOR, clock)) {
                final List<SmartDevice> found = onPoint(known.devices(), point);
                if (found.size() == 1) {
                    return Mono.just(found.getFirst());
                }
            }
            return findAgain(kind, point);
        });
    }

    /**
     * The device as the registry holds it now, not as a kept list remembers it: for a caller that
     * is about to refuse a command because of what the device is.
     */
    public Mono<SmartDevice> findAgain(final DeviceKind kind, final int point) {
        return Mono.defer(() -> {
            final Kept known = kept.get(kind);
            return known != null && known.isYoungerThan(NEW_FOR, clock)
                ? Mono.just(known.devices())
                : read(kind);
        }).map(devices -> theOne(onPoint(devices, point), kind, point));
    }

    //one read for everybody who asks while it is under way
    private Mono<List<SmartDevice>> read(final DeviceKind kind) {
        return reading.computeIfAbsent(kind, asked -> deviceRegistryClient.getDevices(asked)
            .doOnNext(devices -> kept.put(asked, new Kept(devices, clock.instant())))
            .doFinally(signal -> reading.remove(asked))
            .cache());
    }

    //a Shelly or a Bluetooth device of the kind has no Eaton binding and is not looked at
    private static List<SmartDevice> onPoint(final List<SmartDevice> devices, final int point) {
        return devices.stream()
            .filter(device -> device != null
                && device.getEaton() != null
                && device.getEaton().getGateway() != null
                && device.getEaton().getPoint() != null
                && device.getEaton().getPoint() == point)
            .toList();
    }

    private static SmartDevice theOne(final List<SmartDevice> found, final DeviceKind kind, final int point) {
        if (found.isEmpty()) {
            throw new EatonCommandException(
                HttpStatus.NOT_FOUND,
                CustomError.NOT_FOUND_DEVICE,
                "No Eaton device of kind: " + kind.getValue() + " registered on point: " + point
            );
        }
        //the registry holds a point once per gateway, so two devices here are on the two gateways -
        //and the point alone does not say which of them is meant
        if (found.size() > 1) {
            throw new EatonCommandException(
                HttpStatus.CONFLICT,
                CustomError.AMBIGUOUS_POINT,
                "Eaton devices of kind: " + kind.getValue() + " are registered on point: " + point
                    + " of more than one gateway"
            );
        }
        return found.getFirst();
    }

    private record Kept(List<SmartDevice> devices, Instant readAt) {

        private boolean isYoungerThan(final Duration age, final Clock clock) {
            return clock.instant().isBefore(readAt.plus(age));
        }
    }
}

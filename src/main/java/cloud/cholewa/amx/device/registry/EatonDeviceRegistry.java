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
 * The list of a kind is kept for five minutes. A point that is not in a kept list is looked for
 * in a fresh one before it is called unknown, so a device registered a moment ago can be driven
 * at once; a device that was removed or moved to the other gateway is still driven the old way
 * until its list is five minutes old. A failed read of the registry is not kept.
 * <p>
 * Whether a device is {@code enabled} is not looked at: that field is the word of the services
 * that decide what to do with a device, and this one only carries what they decided.
 */
@Component
@RequiredArgsConstructor
public class EatonDeviceRegistry {

    static final Duration KEPT_FOR = Duration.ofMinutes(5);

    private final DeviceRegistryClient deviceRegistryClient;
    private final Clock clock;

    private final Map<DeviceKind, Kept> kept = new ConcurrentHashMap<>();

    public Mono<SmartDevice> find(final DeviceKind kind, final int point) {
        return Mono.defer(() -> {
            final Kept known = kept.get(kind);
            if (known != null && clock.instant().isBefore(known.readAt().plus(KEPT_FOR))) {
                final List<SmartDevice> found = onPoint(known.devices(), point);
                if (!found.isEmpty()) {
                    return Mono.just(found);
                }
            }
            return deviceRegistryClient.getDevices(kind)
                .doOnNext(devices -> kept.put(kind, new Kept(devices, clock.instant())))
                .map(devices -> onPoint(devices, point));
        }).map(found -> theOne(found, kind, point));
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
    }
}

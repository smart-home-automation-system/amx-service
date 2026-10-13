package cloud.cholewa.amx.device.registry;

import cloud.cholewa.amx.device.client.DeviceRegistryClient;
import cloud.cholewa.amx.infrastructure.error.ConfigurationCallException;
import cloud.cholewa.amx.infrastructure.error.CustomError;
import cloud.cholewa.amx.infrastructure.error.EatonCommandException;
import cloud.cholewa.home.model.DeviceKind;
import cloud.cholewa.home.model.EatonBinding;
import cloud.cholewa.home.model.EatonGatewayType;
import cloud.cholewa.home.model.ShellyBinding;
import cloud.cholewa.home.model.SmartDevice;
import cloud.cholewa.home.model.SmartVendor;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.http.HttpStatus;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class EatonDeviceRegistryTest {

    private static final SmartDevice LIVING_ROOM_BLINDS = eaton("Living room blinds", EatonGatewayType.BLINDS, 5);
    private static final SmartDevice OFFICE_BLINDS = eaton("Office blinds", EatonGatewayType.BLINDS, 6);

    @Mock
    private DeviceRegistryClient deviceRegistryClient;

    private final MovableClock clock = new MovableClock();

    private EatonDeviceRegistry sut;

    @BeforeEach
    void setUp() {
        sut = new EatonDeviceRegistry(deviceRegistryClient, clock);
    }

    @Test
    void should_find_the_device_of_the_kind_on_the_point() {
        when(deviceRegistryClient.getDevices(DeviceKind.BLINDS))
            .thenReturn(Mono.just(List.of(LIVING_ROOM_BLINDS, OFFICE_BLINDS)));

        sut.find(DeviceKind.BLINDS, 6)
            .as(StepVerifier::create)
            .expectNext(OFFICE_BLINDS)
            .verifyComplete();
    }

    @Test
    void should_keep_the_list_of_a_kind_for_five_minutes() {
        when(deviceRegistryClient.getDevices(DeviceKind.BLINDS))
            .thenReturn(Mono.just(List.of(LIVING_ROOM_BLINDS, OFFICE_BLINDS)));

        sut.find(DeviceKind.BLINDS, 5).as(StepVerifier::create).expectNext(LIVING_ROOM_BLINDS).verifyComplete();
        clock.advance(Duration.ofMinutes(5).minusSeconds(1));
        sut.find(DeviceKind.BLINDS, 6).as(StepVerifier::create).expectNext(OFFICE_BLINDS).verifyComplete();

        verify(deviceRegistryClient, times(1)).getDevices(DeviceKind.BLINDS);
    }

    //a device moved to the other gateway is driven the new way once the list is five minutes old
    @Test
    void should_read_the_registry_again_when_the_list_is_five_minutes_old() {
        final SmartDevice moved = eaton("Living room blinds", EatonGatewayType.LIGHTS, 5);
        when(deviceRegistryClient.getDevices(DeviceKind.BLINDS))
            .thenReturn(Mono.just(List.of(LIVING_ROOM_BLINDS)))
            .thenReturn(Mono.just(List.of(moved)));

        sut.find(DeviceKind.BLINDS, 5).as(StepVerifier::create).expectNext(LIVING_ROOM_BLINDS).verifyComplete();
        clock.advance(Duration.ofMinutes(5));
        sut.find(DeviceKind.BLINDS, 5).as(StepVerifier::create).expectNext(moved).verifyComplete();

        verify(deviceRegistryClient, times(2)).getDevices(DeviceKind.BLINDS);
    }

    //a device registered a moment ago is driven at once, not five minutes later
    @Test
    void should_look_for_a_point_missing_from_the_kept_list_in_a_fresh_one() {
        when(deviceRegistryClient.getDevices(DeviceKind.BLINDS))
            .thenReturn(Mono.just(List.of(LIVING_ROOM_BLINDS)))
            .thenReturn(Mono.just(List.of(LIVING_ROOM_BLINDS, OFFICE_BLINDS)));

        sut.find(DeviceKind.BLINDS, 5).as(StepVerifier::create).expectNext(LIVING_ROOM_BLINDS).verifyComplete();
        sut.find(DeviceKind.BLINDS, 6).as(StepVerifier::create).expectNext(OFFICE_BLINDS).verifyComplete();

        verify(deviceRegistryClient, times(2)).getDevices(DeviceKind.BLINDS);
    }

    @Test
    void should_keep_a_list_per_kind() {
        final SmartDevice light = eaton("Kitchen ceiling", EatonGatewayType.LIGHTS, 5);
        when(deviceRegistryClient.getDevices(DeviceKind.BLINDS)).thenReturn(Mono.just(List.of(LIVING_ROOM_BLINDS)));
        when(deviceRegistryClient.getDevices(DeviceKind.LIGHT)).thenReturn(Mono.just(List.of(light)));

        sut.find(DeviceKind.BLINDS, 5).as(StepVerifier::create).expectNext(LIVING_ROOM_BLINDS).verifyComplete();
        sut.find(DeviceKind.LIGHT, 5).as(StepVerifier::create).expectNext(light).verifyComplete();
    }

    @Test
    void should_answer_not_found_for_a_point_without_a_device_of_the_kind() {
        when(deviceRegistryClient.getDevices(DeviceKind.BLINDS)).thenReturn(Mono.just(List.of(LIVING_ROOM_BLINDS)));

        sut.find(DeviceKind.BLINDS, 45)
            .as(StepVerifier::create)
            .expectErrorSatisfies(throwable -> assertThat(throwable)
                .isInstanceOfSatisfying(EatonCommandException.class, exception -> {
                    assertThat(exception.getHttpStatus()).isEqualTo(HttpStatus.NOT_FOUND);
                    assertThat(exception.getError()).isEqualTo(CustomError.NOT_FOUND_DEVICE);
                    assertThat(exception.getMessage())
                        .isEqualTo("No Eaton device of kind: blinds registered on point: 45");
                }))
            .verify();
    }

    //a Shelly device of the kind has no point, and one the registry answered half is not driven
    @Test
    void should_not_take_a_device_without_a_whole_eaton_binding() {
        final SmartDevice shelly = SmartDevice.builder()
            .name("Bedroom blinds").kind(DeviceKind.BLINDS).vendor(SmartVendor.SHELLY)
            .shelly(ShellyBinding.builder().address("192.0.2.17").generation(2).channel(5).build())
            .build();
        final SmartDevice withoutGateway = SmartDevice.builder()
            .name("Half blinds").kind(DeviceKind.BLINDS).vendor(SmartVendor.EATON)
            .eaton(EatonBinding.builder().point(5).build())
            .build();
        when(deviceRegistryClient.getDevices(DeviceKind.BLINDS))
            .thenReturn(Mono.just(List.of(shelly, withoutGateway)));

        sut.find(DeviceKind.BLINDS, 5)
            .as(StepVerifier::create)
            .expectErrorSatisfies(throwable -> assertThat(throwable)
                .isInstanceOfSatisfying(EatonCommandException.class, exception ->
                    assertThat(exception.getError()).isEqualTo(CustomError.NOT_FOUND_DEVICE)))
            .verify();
    }

    //the point alone does not say which gateway is meant, and a frame is not sent on a guess
    @Test
    void should_refuse_a_point_that_holds_a_device_of_the_kind_on_both_gateways() {
        when(deviceRegistryClient.getDevices(DeviceKind.BLINDS)).thenReturn(Mono.just(List.of(
            LIVING_ROOM_BLINDS, eaton("Garage blinds", EatonGatewayType.LIGHTS, 5)
        )));

        sut.find(DeviceKind.BLINDS, 5)
            .as(StepVerifier::create)
            .expectErrorSatisfies(throwable -> assertThat(throwable)
                .isInstanceOfSatisfying(EatonCommandException.class, exception -> {
                    assertThat(exception.getHttpStatus()).isEqualTo(HttpStatus.CONFLICT);
                    assertThat(exception.getError()).isEqualTo(CustomError.AMBIGUOUS_POINT);
                }))
            .verify();
    }

    @Test
    void should_not_keep_a_failed_read_of_the_registry() {
        when(deviceRegistryClient.getDevices(DeviceKind.BLINDS))
            .thenReturn(Mono.error(new ConfigurationCallException(HttpStatus.BAD_GATEWAY, Set.of())))
            .thenReturn(Mono.just(List.of(LIVING_ROOM_BLINDS)));

        sut.find(DeviceKind.BLINDS, 5)
            .as(StepVerifier::create)
            .expectError(ConfigurationCallException.class)
            .verify();
        sut.find(DeviceKind.BLINDS, 5).as(StepVerifier::create).expectNext(LIVING_ROOM_BLINDS).verifyComplete();
    }

    private static SmartDevice eaton(final String name, final EatonGatewayType gateway, final int point) {
        return SmartDevice.builder()
            .name(name)
            .kind(DeviceKind.BLINDS)
            .vendor(SmartVendor.EATON)
            .enabled(true)
            .eaton(EatonBinding.builder().gateway(gateway).point(point).build())
            .build();
    }

    private static final class MovableClock extends Clock {

        private Instant now = Instant.parse("2026-10-13T18:00:00Z");

        void advance(final Duration duration) {
            now = now.plus(duration);
        }

        @Override
        public Instant instant() {
            return now;
        }

        @Override
        public ZoneId getZone() {
            return ZoneId.of("Europe/Warsaw");
        }

        @Override
        public Clock withZone(final ZoneId zone) {
            return this;
        }
    }
}

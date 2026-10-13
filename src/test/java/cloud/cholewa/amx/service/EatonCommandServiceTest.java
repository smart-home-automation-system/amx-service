package cloud.cholewa.amx.service;

import cloud.cholewa.amx.device.registry.EatonDeviceRegistry;
import cloud.cholewa.amx.eaton.frame.BlindsCommand;
import cloud.cholewa.amx.frameserver.FrameServerClient;
import cloud.cholewa.amx.infrastructure.error.CustomError;
import cloud.cholewa.amx.infrastructure.error.EatonCommandException;
import cloud.cholewa.home.model.DeviceKind;
import cloud.cholewa.home.model.EatonBinding;
import cloud.cholewa.home.model.EatonGatewayType;
import cloud.cholewa.home.model.LightKind;
import cloud.cholewa.home.model.RoomName;
import cloud.cholewa.home.model.SmartDevice;
import cloud.cholewa.home.model.SmartVendor;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.NullSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.http.HttpStatus;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.time.Clock;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class EatonCommandServiceTest {

    //half a second past the minute, in summer time: the answer is the local time of the house, to the second
    private static final Clock CLOCK = Clock.fixed(Instant.parse("2026-07-01T16:30:05.500Z"), ZoneId.of("Europe/Warsaw"));
    private static final LocalDateTime SENT_AT = LocalDateTime.of(2026, 7, 1, 18, 30, 5);

    @Mock
    private EatonDeviceRegistry eatonDeviceRegistry;

    @Mock
    private FrameServerClient frameServerClient;

    private EatonCommandService sut;

    @BeforeEach
    void setUp() {
        sut = new EatonCommandService(eatonDeviceRegistry, frameServerClient, CLOCK);
    }

    @Test
    void should_send_the_blinds_frame_to_the_gateway_of_the_registry() {
        when(eatonDeviceRegistry.find(DeviceKind.BLINDS, 5))
            .thenReturn(Mono.just(device(DeviceKind.BLINDS, EatonGatewayType.BLINDS, 5, null)));
        when(frameServerClient.send(EatonGatewayType.BLINDS, "5A,09,B1,05,0E,02,00,00,00,00,A5"))
            .thenReturn(Mono.empty());

        sut.moveBlinds(5, BlindsCommand.STOP)
            .as(StepVerifier::create)
            .expectNext(SENT_AT)
            .verifyComplete();
    }

    //the gateway is the one the registry names, whatever the kind of the device suggests
    @Test
    void should_send_the_frame_of_a_light_to_the_gateway_it_is_registered_on() {
        when(eatonDeviceRegistry.find(DeviceKind.LIGHT, 7))
            .thenReturn(Mono.just(device(DeviceKind.LIGHT, EatonGatewayType.BLINDS, 7, LightKind.RELAY)));
        when(frameServerClient.send(EatonGatewayType.BLINDS, "5A,09,B1,07,0A,01,00,00,00,00,A5"))
            .thenReturn(Mono.empty());

        sut.switchLight(7, true)
            .as(StepVerifier::create)
            .expectNext(SENT_AT)
            .verifyComplete();
    }

    @Test
    void should_send_the_level_to_a_dimmer() {
        when(eatonDeviceRegistry.find(DeviceKind.LIGHT, 12))
            .thenReturn(Mono.just(device(DeviceKind.LIGHT, EatonGatewayType.LIGHTS, 12, LightKind.DIMMER)));
        when(frameServerClient.send(EatonGatewayType.LIGHTS, "5A,09,B1,0C,0C,32,00,00,00,00,A5"))
            .thenReturn(Mono.empty());

        sut.setLightLevel(12, 50)
            .as(StepVerifier::create)
            .expectNext(SENT_AT)
            .verifyComplete();
    }

    @ParameterizedTest
    @NullSource
    @EnumSource(value = LightKind.class, names = "DIMMER", mode = EnumSource.Mode.EXCLUDE)
    void should_not_send_a_level_to_a_light_that_is_not_registered_as_a_dimmer(final LightKind lightKind) {
        when(eatonDeviceRegistry.find(DeviceKind.LIGHT, 12))
            .thenReturn(Mono.just(device(DeviceKind.LIGHT, EatonGatewayType.LIGHTS, 12, lightKind)));
        when(eatonDeviceRegistry.findAgain(DeviceKind.LIGHT, 12))
            .thenReturn(Mono.just(device(DeviceKind.LIGHT, EatonGatewayType.LIGHTS, 12, lightKind)));

        sut.setLightLevel(12, 50)
            .as(StepVerifier::create)
            .expectErrorSatisfies(throwable -> assertThat(throwable)
                .isInstanceOfSatisfying(EatonCommandException.class, exception -> {
                    assertThat(exception.getHttpStatus()).isEqualTo(HttpStatus.CONFLICT);
                    assertThat(exception.getError()).isEqualTo(CustomError.NOT_A_DIMMER);
                }))
            .verify();

        verifyNoInteractions(frameServerClient);
    }

    //the kept list still calls the light a relay; the owner made it a dimmer a moment ago
    @Test
    void should_read_the_device_again_before_it_refuses_a_level() {
        when(eatonDeviceRegistry.find(DeviceKind.LIGHT, 12))
            .thenReturn(Mono.just(device(DeviceKind.LIGHT, EatonGatewayType.LIGHTS, 12, LightKind.RELAY)));
        when(eatonDeviceRegistry.findAgain(DeviceKind.LIGHT, 12))
            .thenReturn(Mono.just(device(DeviceKind.LIGHT, EatonGatewayType.LIGHTS, 12, LightKind.DIMMER)));
        when(frameServerClient.send(EatonGatewayType.LIGHTS, "5A,09,B1,0C,0C,32,00,00,00,00,A5"))
            .thenReturn(Mono.empty());

        sut.setLightLevel(12, 50)
            .as(StepVerifier::create)
            .expectNext(SENT_AT)
            .verifyComplete();
    }

    @Test
    void should_send_nothing_for_a_point_the_registry_does_not_know() {
        final EatonCommandException unknown =
            new EatonCommandException(HttpStatus.NOT_FOUND, CustomError.NOT_FOUND_DEVICE, "no device");
        when(eatonDeviceRegistry.find(DeviceKind.BLINDS, 45)).thenReturn(Mono.error(unknown));

        sut.moveBlinds(45, BlindsCommand.OPEN)
            .as(StepVerifier::create)
            .expectErrorSatisfies(throwable -> assertThat(throwable).isSameAs(unknown))
            .verify();

        verifyNoInteractions(frameServerClient);
    }

    //no time of sending for a frame the server did not take
    @Test
    void should_fail_with_the_failure_of_the_frame_server() {
        final EatonCommandException silent =
            new EatonCommandException(HttpStatus.GATEWAY_TIMEOUT, CustomError.FRAME_SERVER_TIMEOUT, "silent");
        when(eatonDeviceRegistry.find(DeviceKind.LIGHT, 7))
            .thenReturn(Mono.just(device(DeviceKind.LIGHT, EatonGatewayType.LIGHTS, 7, LightKind.RELAY)));
        when(frameServerClient.send(EatonGatewayType.LIGHTS, "5A,09,B1,07,0A,00,00,00,00,00,A5"))
            .thenReturn(Mono.error(silent));

        sut.switchLight(7, false)
            .as(StepVerifier::create)
            .expectErrorSatisfies(throwable -> assertThat(throwable).isSameAs(silent))
            .verify();

        verify(frameServerClient).send(EatonGatewayType.LIGHTS, "5A,09,B1,07,0A,00,00,00,00,00,A5");
    }

    private static SmartDevice device(
        final DeviceKind kind,
        final EatonGatewayType gateway,
        final int point,
        final LightKind lightKind
    ) {
        return SmartDevice.builder()
            .name("Device on point " + point)
            .room(RoomName.LIVING_ROOM)
            .kind(kind)
            .vendor(SmartVendor.EATON)
            .enabled(true)
            .eaton(EatonBinding.builder().gateway(gateway).point(point).build())
            .lightKind(lightKind)
            .build();
    }
}

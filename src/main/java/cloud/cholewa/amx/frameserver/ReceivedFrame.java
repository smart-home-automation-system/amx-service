package cloud.cholewa.amx.frameserver;

import cloud.cholewa.home.model.EatonGatewayType;

//a frame one of the Eaton gateways gave the AMX master, as the frame server sent it: the gateway
//is the prefix of the line, the frame the comma-separated hex after it, from 5A to A5
public record ReceivedFrame(EatonGatewayType gateway, String frame) {
}

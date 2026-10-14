package cloud.cholewa.amx.frameserver;

import cloud.cholewa.home.model.EatonGatewayType;

import java.time.LocalDateTime;

//a frame one of the Eaton gateways gave the AMX master, as the frame server sent it: the gateway
//is the prefix of the line, the frame the comma-separated hex after it, from 5A to A5, and the
//time is the moment the line arrived here - what is published carries that time, not the time
//of the publishing: a backlog behind a lookup that failed for a while is old news
public record ReceivedFrame(EatonGatewayType gateway, String frame, LocalDateTime receivedAt) {
}

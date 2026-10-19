# amx-service

[![CI](https://github.com/smart-home-automation-system/amx-service/actions/workflows/CI.yml/badge.svg)](https://github.com/smart-home-automation-system/amx-service/actions/workflows/CI.yml)
[![Quality Gate Status](https://sonarcloud.io/api/project_badges/measure?project=smart-home-automation-system_amx-service&metric=alert_status)](https://sonarcloud.io/summary/new_code?id=smart-home-automation-system_amx-service)
[![Vulnerabilities](https://sonarcloud.io/api/project_badges/measure?project=smart-home-automation-system_amx-service&metric=vulnerabilities)](https://sonarcloud.io/summary/new_code?id=smart-home-automation-system_amx-service)

![GitHub Release Date - Published_At](https://img.shields.io/github/release-date/smart-home-automation-system/amx-service?style=plastic)
![GitHub Release](https://img.shields.io/github/v/release/smart-home-automation-system/amx-service?style=plastic)
---
![GitHub top language](https://img.shields.io/github/languages/top/smart-home-automation-system/amx-service?style=plastic)
![Java](https://img.shields.io/badge/java-21-yellow?style=plastic)
![SpringBoot](https://img.shields.io/badge/SpringBoot-4.1.1-blue?style=plastic)
[![Coverage](https://sonarcloud.io/api/project_badges/measure?project=smart-home-automation-system_amx-service&metric=coverage)](https://sonarcloud.io/summary/new_code?id=smart-home-automation-system_amx-service)
[![Lines of Code](https://sonarcloud.io/api/project_badges/measure?project=smart-home-automation-system_amx-service&metric=ncloc)](https://sonarcloud.io/summary/new_code?id=smart-home-automation-system_amx-service)

![GitHub issues](https://img.shields.io/github/issues/smart-home-automation-system/amx-service?style=plastic)
![GitHub contributors](https://img.shields.io/github/contributors/smart-home-automation-system/amx-service?style=plastic)
![GitHub pull requests](https://img.shields.io/github/issues-pr-raw/smart-home-automation-system/amx-service?style=plastic)

![GitHub last commit](https://img.shields.io/github/last-commit/smart-home-automation-system/amx-service?style=plastic)
![GitHub commit activity](https://img.shields.io/github/commit-activity/m/smart-home-automation-system/amx-service?style=plastic)

---

# Description

The general purpose of this service is two-way communication with an AMX control system and its connected devices.

The AMX master runs a **frame server** (a TCP server in NetLinx, port 20010) that moves bytes
between this service and the two Eaton wireless gateways of the house, both ways, over one
permanent connection. This service is its only client:

- **In**: every frame a gateway receives (a room sensor reporting, a button pressed, an
  actuator telling its state) arrives as one line. The service validates it, asks
  `database-service` which device the data point belongs to, and publishes the result on
  RabbitMQ for the services that act on it — the room temperatures for `heating-service`,
  and what an Eaton blind, light or dimmer reports as a device event for the services of the
  devices (see [Messaging](#messaging)).
- **Out**: a command for an Eaton blinds or light actuator is turned into the raw frame of the
  gateway and sent as one line; the master writes it to the serial gateway (see
  [Eaton commands](#eaton-commands)).

The line protocol of the socket is pinned next to the NetLinx module, in the `CLAUDE.md` of the
`amx` repository; what this side does with it is in [The frame server](#the-frame-server).

## Run locally

```bash
mvn verify                                  # build and tests
mvn spring-boot:run -Dspring-boot.run.profiles=home,local
```

| | Application | Actuator |
|---|---|---|
| local (`local` profile) | 6001 | 8001 |
| cluster (`home` profile) | 6200 | 8200 |

The `local` profile expects `database-service` on `localhost:6005` and RabbitMQ on
`localhost:5672`; the Actuator exposes `health`, `info` and `prometheus`. It looks for the frame
server on `localhost:20010`, where nothing listens unless a stand-in is started — a local run
receives nothing and cannot move a blind by accident; the client keeps trying, with a backoff.

| Property | Default | |
|---|---|---|
| `amx.frame-server.host` | none — **mandatory** | the AMX master; in the cluster from `AMX_FRAME_SERVER_HOST` of the manifest, and a pod without it does not start |
| `amx.frame-server.port` | `20010` | |
| `amx.frame-server.connect-timeout` | `2s` | one attempt to connect; the attempts go on, 1 s to 30 s apart, until one succeeds. 1 to 5 s, a bare number is seconds |
| `amx.frame-server.response-timeout` | `3s` | the wait for the answer to a frame that was sent; 1 to 10 s, a bare number is seconds |

| `device-events.password` | none — **mandatory** | the broker user `device` of the vhost `/device`; in the cluster from `DEVICE_EVENTS_PASSWORD` of the manifest, and a pod without it does not start |
| `device-events.confirm-timeout` | `5s` | how long publishing one device event may take, the send and the confirmation of the broker together; 1 to 10 s, a bare number is seconds |

The lookup in `database-service` is bounded by `internal.service.database.response-timeout`
(default `PT5S`).

### Health

`/actuator/health` carries `frameServer`: `UP` while the socket to the master is open, `DOWN`
with `connected: false` while it is not. It is **not** part of the readiness or the liveness
group — a master that is switched off must neither take the pod out of the Service nor restart
it in a loop; the client connects again by itself.

## API

Base path `/home` (`spring.webflux.base-path`); in the cluster the ingress routes
`/home/amx` to the service. The frames of the gateways do not come through HTTP any more
(until 1.4.0 the master posted them to `POST /home/amx`): they arrive through the socket.

| Method | Path | Description |
|---|---|---|
| POST | `/home/amx/eaton/blinds/{point}?command=open\|close\|stop` | Move the Eaton blinds on a data point |
| POST | `/home/amx/eaton/blinds/{point}/status` | Ask the Eaton blinds actuator on a data point for its status (HAS-291) |
| POST | `/home/amx/eaton/lights/{point}?turn=on\|off` | Switch the Eaton light on a data point |
| POST | `/home/amx/eaton/lights/{point}/status` | Ask the Eaton switching or dimming actuator on a data point for its status (HAS-259) |
| POST | `/home/amx/eaton/lights/{point}/level?value=0..100` | Set the level, in percent, of the Eaton dimmer on a data point |

### Eaton commands

The five writes answer **202** with `{"sentAt": "2026-10-13T20:06:44"}` — the local time of the
house at which the frame server took the frame. 202 and not 200: the answer says the frame was
handed to the gateway, never that the device did what it was told. What the device did is told
by its own frame, which arrives like every other one.

What a caller has to know:

- **`point` is 1 to 99, and the gateway is not named**: it comes from the device registry of
  `database-service` (`GET /home/device/registry?kind=`, 0.12.0 or later). The device has to be
  registered with the kind the command is for — `blinds` or `light` — and an Eaton binding; a
  point that holds a room sensor is not sent a blinds frame.
- **`command` is `open`, `close` or `stop`, `turn` is `on` or `off`, exactly as written.** Anything
  else is a 400 — a typo moves nothing and switches nothing off.
- **A level is for a dimmer**: a light whose `lightKind` in the registry is `dimmer`.
- **A status request of a light** (HAS-259) is the same `0B` for a point the registry holds as a
  `light`: the actuator answers with its status - a switch `0` / `1`, a dimmer its level -
  published as a `status` event; `light-service` asks every Eaton light once after its start.
- **A status request is for a blind** (HAS-291) and takes no parameter: it sends the event `0B`
  (data `00`), never a move. The answer is the actuator's usual status frame - `1` opening, `2`
  closing, `0` stopped - published as a `status` event like any other; the 202 says only that the
  request went out. Asked after the end of a run the actuator says it stopped, which tells no
  direction - `blinds-service` asks within seconds of a command whose movement was not reported.
- **The registry is kept for five minutes.** A device that was just registered or corrected is
  driven at once (before a command is refused the registry is read again, at most once in ten
  seconds); a device that was removed, or moved to the other gateway, is driven the old way for
  up to five minutes.
- **`enabled` is not looked at.** This service carries what the caller decided.
- **The commands go to the master one after the other**, in the order they came; a command that
  waits behind others waits for them too. A command while the socket is not open is refused at
  once — the frame was not sent.
- **A frame is never sent twice.** When the answer of the server does not come, the caller is
  told and decides — the `details` say whether the frame *was not sent* or *may have been
  carried out*.
- **A caller that gives up is not served later**: a command whose request was cancelled before
  its line was written is dropped.
- They are reachable from outside through the gateway (`/amx/**`), unauthenticated like the rest.

Failures come in the error contract of `cholewa-commons`, with a `code` to branch on:

| Status | `code` | When |
|---|---|---|
| 400 | — | `point` outside 1–99, `value` outside 0–100, an unknown `command` or `turn`, a missing parameter |
| 404 | `NOT_FOUND_DEVICE` | no Eaton device of the kind is registered on the point |
| 409 | `AMBIGUOUS_POINT` | a device of the kind is registered on that point of both gateways |
| 409 | `NOT_A_DIMMER` | a level for a light the registry does not call a dimmer |
| 502 | `FRAME_SERVER_UNAVAILABLE` | the socket to the frame server is not open (the frame was not sent), or the connection was lost before its answer |
| 502 | `FRAME_REFUSED` | the frame server answered `ERR`; its reason is in `details` |
| 504 | `FRAME_SERVER_TIMEOUT` | the frame server did not answer within `response-timeout`; the frame may have been carried out |
| 502 / 504 | — | the registry of `database-service` failed or did not answer |

The 5xx are logged at ERROR, the 4xx at WARN.

### The frame server

A TCP server on the AMX master (the NetLinx module `Gateway_SpringBoot_REST_nl3_0_0` on the
Television master, port 20010). Text, one message per line, ended by LF, the same shape in both
directions: a **frame line** is `<gateway>;<frame>` — `blinds` or `lights`, then the whole serial
frame from `5A` to `A5`, every byte as two hex digits, comma-separated.

- **From the master**: every frame either gateway received, the moment it is complete, in the
  order received — nothing is filtered or parsed on the master.
- **To the master**: a frame line is a command; the master writes the bytes to that gateway and
  answers exactly one line, `OK` (the gateway got the bytes — not that the RF telegram was sent;
  the gateway's own acknowledgement arrives as a frame line) or `ERR <reason>`.
- A frame line may arrive at any moment, also between a command and its answer. A line that is
  neither a frame nor `OK` / `ERR` is logged and ignored — never taken for an answer.

The client connects when the application starts and connects again, with a backoff, whenever
the connection ends: it is the only way to receive. A connection nothing arrives on for five
minutes is taken for dead and opened again — a master that was power-cycled sends no FIN. The
frames this service sends are the TX packet of the gateway's protocol as documented (length
`06`):

| Command | Frame |
|---|---|
| blinds open / close / stop | `5A,06,B1,<point>,0E,<01 \| 00 \| 02>,00,A5` |
| status request (blinds or light) | `5A,06,B1,<point>,0B,00,00,A5` |
| light on / off | `5A,06,B1,<point>,0A,<01 \| 00>,00,A5` |
| dimmer level | `5A,06,B1,<point>,0C,<00–64>,00,A5` |

The point and the level are one byte each, written in two hex digits.

### What becomes of a received frame

The type byte decides first. An RX frame (`C1`, a device reporting) is looked up in
`database-service` by its data point and gateway, and the reading of a temperature sensor is
published, dated with the moment the frame arrived; a STATUS frame (`C3`, the gateway's answer
to a command — this service's or the other master's) and an ERROR frame (`C9`, a text of the
gateway) are logged and never looked up. A status says whether the gateway sent the telegram:
`1C 04` is sent (INFO); `09 01`–`09 04` is not sent, with the reason — unknown command, data
point out of range, RF busy; `09 06` is sent and acknowledged by no actuator (nothing on that
point, or out of range); `09 05` and any other error by its number (all WARN; the caller of the
command already got its 202, this line is all that tells it). A frame of a configured blinds,
light, dimmer, push button or motion sensor is published as a device event (see
[Messaging](#messaging)); a device of the type `other` is logged (WARN) and dropped.

Nothing a frame does can stop the next one. Every failure is one line in the log, at the level
the alerts expect:

| Level | When |
|---|---|
| WARN | the parser refuses the frame (also the frame of an actuator with a message type that has no event, or an analog value that is not a whole number); `database-service` knows no device for the data point (its 404 carries the code `NOT_FOUND_DEVICE_CONFIGURATION`) |
| ERROR | `database-service` fails or cannot be reached (any 5xx, any other 4xx, a 404 without that code, an answer without a configuration or one that cannot be read), or does not answer within the timeout; a device event the broker did not take (not sent, refused, routed to no queue, or not confirmed within `device-events.confirm-timeout`); anything unexpected |

What a failing `database-service` said about the failure is in that line, after `downstream:`.
The lookup needs `database-service` 0.8.0 or later — against an older one every unknown data
point reads as an outage. Each frame is processed under an observation of its own, so the lines
it writes share one `traceId`.

## Messaging

| Direction | Exchange / queue | vhost | Payload |
|---|---|---|---|
| publishes | fanout exchange `temperature.events` | `/temperature` | `TemperatureMessage` (room, temperature, date) |
| publishes | topic exchange `amq.topic`, routing key `eaton.<gateway>.<point>` | `/device` | `DeviceEvent` of `smart-home-sdk` |

**Device events** (since HAS-229): what an Eaton blind, light, dimmer, wall push button or PIR
sensor reports — its status after a command, a button pressed, a motion detected (push buttons
and PIR sensors since HAS-288) — published over a second connection (`<pod>/device`, user
`device`) with publisher confirms. The event names the device as the frame does, by gateway and
point; the consumer looks it up in the device registry. Every `device.<env>.<domain>` queue is
bound with `#`, so each of them gets every event.

```json
{"at":"2026-10-15T18:30:05","vendor":"eaton","gateway":"blinds","point":14,"event":"status","value":1}
```

`value` comes with `status` — blinds `0` stop, `1` open, `2` close; a switch `0` off, `1` on; a
dimmer its level 0–100 — and with an `analog value` that is a whole number; the other events
(`push button on`, `up pressed`, …) carry none. A wall push button reports `push button on` /
`push button off`, a PIR sensor `switch on` / `switch off`. `at` is the moment the frame
arrived. A device is published only when the registry holds its point as blinds, light,
dimmer, push button or motion sensor (`smart-home-sdk` 1.9.0 or later, which this service must
run before `database-service` answers the last two).

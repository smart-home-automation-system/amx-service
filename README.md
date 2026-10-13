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

The AMX/NetLinx controller forwards every datagram it receives from the Eaton wireless gateways
(blinds, lights, sensors) to this service. The service validates the frame, parses it into a
domain message, asks `database-service` which device the data point belongs to, and publishes the
result on RabbitMQ for the services that act on it — today the room temperatures consumed by
`heating-service`.

The other direction exists too: a command for an Eaton blinds or light actuator is turned into
the raw frame of the gateway and handed to the **frame server** of the AMX master, a TCP server
in NetLinx that writes it to the serial gateway (see [Eaton commands](#eaton-commands)).

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
server on `localhost:20010`, where nothing listens unless a stub is started — a local run cannot
move a blind by accident.

| Property | Default | |
|---|---|---|
| `amx.frame-server.host` | none — **mandatory** | the AMX master; in the cluster from `AMX_FRAME_SERVER_HOST` of the manifest, and a pod without it does not start |
| `amx.frame-server.port` | `20010` | |
| `amx.frame-server.connect-timeout` | `2s` | one attempt to connect, of up to three; 1 to 5 s, a bare number is seconds |
| `amx.frame-server.response-timeout` | `3s` | the wait for the answer to a frame; 1 to 10 s, a bare number is seconds |

The lookup in `database-service` is bounded by `internal.service.database.response-timeout`
(default `PT5S`, well below the gateway's 30 s); a lookup that runs out logs
`Error processing configuration call: database-service did not answer within …` instead of
holding the AMX request until the gateway gives up.

When the lookup fails, `POST /home/amx` answers with a status that says whose problem it is:
`404` when `database-service` knows no device for the data point — that is, when its 404
carries the code `NOT_FOUND_DEVICE_CONFIGURATION` — `502` when `database-service` fails or
cannot be reached (any 5xx, any other 4xx, a 404 without that code, connection refused, an
answer without a configuration or one that cannot be read) and `504` when it does not answer
within the timeout. A malformed frame stays `400`.

Only the 404 of an unknown data point repeats what `database-service` said. A `502` names the
status `database-service` answered and nothing more — the messages of a failing service are
raw exception text, so they go to the log line and stay out of the response.

A 404 without the code is a path nothing answers under (a renamed endpoint, a broken route),
not an unknown data point: it is logged at ERROR, where the alerts look, instead of passing as
a WARN about a missing device. The lookup therefore needs `database-service` 0.8.0 or later —
against an older one every unknown data point reads as a 502.

## API

Base path `/home` (`spring.webflux.base-path`); in the cluster the ingress routes
`/home/amx` to the service.

| Method | Path | Description |
|---|---|---|
| POST | `/home/amx` | Consume an Eaton datagram (`EatonDatagramReply`: gateway + raw frame) forwarded by the AMX controller; returns 200 once the frame has been parsed and published |
| POST | `/home/amx/eaton/blinds/{point}?command=open\|close\|stop` | Move the Eaton blinds on a data point |
| POST | `/home/amx/eaton/lights/{point}?turn=on\|off` | Switch the Eaton light on a data point |
| POST | `/home/amx/eaton/lights/{point}/level?value=0..100` | Set the level, in percent, of the Eaton dimmer on a data point |

### Eaton commands

The three writes answer **202** with `{"sentAt": "2026-10-13T20:06:44"}` — the local time of the
house at which the frame server took the frame. 202 and not 200: the answer says the frame was
handed to the gateway, never that the device did what it was told. What the device did is told
by its own datagram.

What a caller has to know:

- **`point` is 1 to 99, and the gateway is not named**: it comes from the device registry of
  `database-service` (`GET /home/device/registry?kind=`, 0.12.0 or later). The device has to be
  registered with the kind the command is for — `blinds` or `light` — and an Eaton binding; a
  point that holds a room sensor is not sent a blinds frame.
- **`command` is `open`, `close` or `stop`, `turn` is `on` or `off`, exactly as written.** Anything
  else is a 400 — a typo moves nothing and switches nothing off.
- **A level is for a dimmer**: a light whose `lightKind` in the registry is `dimmer`.
- **The registry is kept for five minutes.** A device that was just registered or corrected is
  driven at once (before a command is refused the registry is read again, at most once in ten
  seconds); a device that was removed, or moved to the other gateway, is driven the old way for
  up to five minutes.
- **`enabled` is not looked at.** This service carries what the caller decided.
- **The commands go to the master one after the other**, in the order they came. Up to about
  10 s for one that fails with the defaults (three attempts to connect and the wait for the
  answer); a command that waits behind others waits for them too.
- **A frame is never sent twice.** Only the connecting is repeated. When the answer of the server
  does not come, the caller is told and decides — the `details` say whether the frame *was not
  sent* or *may have been carried out*.
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
| 502 | `FRAME_SERVER_UNAVAILABLE` | the frame server was not reached (the frame was not sent), the connection was lost before its answer, or it answered something that is neither `OK` nor `ERR` |
| 502 | `FRAME_REFUSED` | the frame server answered `ERR`; its reason is in `details` |
| 504 | `FRAME_SERVER_TIMEOUT` | the frame server did not answer within `response-timeout` |
| 502 / 504 | — | the registry of `database-service` failed or did not answer, as for a datagram |

The 5xx are logged at ERROR, the 4xx at WARN.

### The frame server

A TCP server on the AMX master (NetLinx module `Eaton_FrameServer`, port 20010). A command is
one line, `<gateway>;<frame>` ended by LF — `blinds;5A,09,B1,05,0E,01,00,00,00,00,A5` — and the
server answers one line, `OK` or `ERR <reason>`. It has to send that one line and nothing else:
the answers carry nothing that ties them to a command, so a greeting or a second line would be
read as the answer of a command.

| Command | Frame |
|---|---|
| blinds open / close / stop | `5A,09,B1,<point>,0E,<01 \| 00 \| 02>,00,00,00,00,A5` |
| light on / off | `5A,09,B1,<point>,0A,<01 \| 00>,00,00,00,00,A5` |
| dimmer level | `5A,09,B1,<point>,0C,<00–64>,00,00,00,00,A5` |

The point and the level are one byte each, written in two hex digits.

## Messaging

| Direction | Exchange / queue | vhost | Payload |
|---|---|---|---|
| publishes | fanout exchange `temperature.events` | `/temperature` | `TemperatureMessage` (room, temperature, date) |

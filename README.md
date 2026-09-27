# amx-service

[![CI](https://github.com/smart-home-automation-system/amx-service/actions/workflows/CI.yml/badge.svg)](https://github.com/smart-home-automation-system/amx-service/actions/workflows/CI.yml)
[![Quality Gate Status](https://sonarcloud.io/api/project_badges/measure?project=smart-home-automation-system_amx-service&metric=alert_status)](https://sonarcloud.io/summary/new_code?id=smart-home-automation-system_amx-service)
[![Vulnerabilities](https://sonarcloud.io/api/project_badges/measure?project=smart-home-automation-system_amx-service&metric=vulnerabilities)](https://sonarcloud.io/summary/new_code?id=smart-home-automation-system_amx-service)

![GitHub Release Date - Published_At](https://img.shields.io/github/release-date/smart-home-automation-system/amx-service?style=plastic)
![GitHub Release](https://img.shields.io/github/v/release/smart-home-automation-system/amx-service?style=plastic)
---
![GitHub top language](https://img.shields.io/github/languages/top/smart-home-automation-system/amx-service?style=plastic)
![Java](https://img.shields.io/badge/java-21-yellow?style=plastic)
![SpringBoot](https://img.shields.io/badge/SpringBoot-4.1.0-blue?style=plastic)
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
`heating-service` and `notification-service`.

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
`localhost:5672`; the Actuator exposes `health`, `info` and `prometheus`.

The lookup in `database-service` is bounded by `internal.service.database.response-timeout`
(default `PT5S`, well below the gateway's 30 s); a lookup that runs out logs
`Error processing configuration call: database-service did not answer within …` instead of
holding the AMX request until the gateway gives up.

When the lookup fails, `POST /home/amx` answers with a status that says whose problem it is:
`404` when `database-service` knows no device for the data point, `502` when
`database-service` fails or cannot be reached (any 5xx, any 4xx other than 404, an error body
that is not the `Errors` contract, connection refused) and `504` when it does not answer within
the timeout. A malformed frame stays `400`. The log line names the status `database-service`
answered.

## API

Base path `/home` (`spring.webflux.base-path`); in the cluster the ingress routes
`/home/amx` to the service.

| Method | Path | Description |
|---|---|---|
| POST | `/home/amx` | Consume an Eaton datagram (`EatonDatagramReply`: gateway + raw frame) forwarded by the AMX controller; returns 200 once the frame has been parsed and published |

## Messaging

| Direction | Exchange / queue | vhost | Payload |
|---|---|---|---|
| publishes | fanout exchange `temperature.events` | `/temperature` | `TemperatureMessage` (room, temperature, date) |

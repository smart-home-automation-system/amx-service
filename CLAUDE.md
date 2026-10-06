# amx-service

`cloud.cholewa:amx-service` — the bridge between the AMX/NetLinx control system and the backend:
the AMX controller forwards every datagram it receives from the Eaton wireless gateways, this
service parses it, finds out which device the data point is and publishes the result on
RabbitMQ. Reactive (WebFlux), Java 21, Spring Boot 4.1.1 (`spring-boot-starter-parent`), Maven.
Local port **6001** (management **8001**); in the deployed `home` profile **6200** with Actuator
on **8200**. Docker image `magikabdul/amx-service`; the pom keeps `0.0.1-SNAPSHOT`, the released
version comes from the git tag. Not to be confused with `amx` (the NetLinx sources of the
controller itself, on the personal account) or with `api-gateway-service` (the HTTP edge).

Org-wide conventions and working rules (PR flow, branch naming `feature/HAS-<n>`, "user writes
service code, Claude reviews", both reviews before merge, own libraries on their latest
release) live in the workspace `organization.md` — this file only covers what is specific to
this repo. When opened as part of the workspace, those rules apply here too.

## Flow

`POST /home/amx` (`EatonDatagramReply`: gateway + raw frame, sent by the AMX controller through
the ingress → `api-gateway-service`) → `AmxController` → `AmxService`:

1. validate and split the frame (`eaton/utils` — `MessageValidator`, `MessageUtilities`);
2. look the data point up in `database-service` (`DeviceDatabaseClient`,
   `GET /home/device/configuration/eaton?point=&gateway=`) — roughly every 30 s, day and night;
3. by the configured device type: `TEMPERATURE_SENSOR` → `TemperaturePublisher` publishes a
   `TemperatureMessage` to the fanout exchange `temperature.events` (vhost `/temperature`),
   consumed by `heating-service` alone (queue `temperature.prod.heating`). Blinds, lights, dimmers and
   `OTHER` are **not supported yet** and end in a `RuntimeException`, which the default
   processor renders as 500 and logs at ERROR.

## The call to database-service — read before touching `DeviceDatabaseClient`

- **Bounded by `internal.service.database.response-timeout`** (default `PT5S`, HAS-150). Without
  it every datagram waited until the gateway's 30 s 504 during the 2026-09-26 outage. Keep it
  well above `database-service`'s pool `max-validation-time` (2 s, `cholewa-commons` ≥ 1.5.1):
  while the pool discards broken connections, a request can legitimately take a few seconds.
- **The status comes from the HTTP response, the cause from the code in the body** (HAS-176).
  The error response is read with `DownstreamErrors.read` of `cholewa-commons` (≥ 1.7.0), which
  keeps the status whatever the body is; there is no body parsing here any more. A 404 is
  relayed **only with `code = NOT_FOUND_DEVICE_CONFIGURATION`** (= unknown data point). A 404
  without it is a path nothing answers under — a renamed endpoint, a broken route — and until
  HAS-176 it passed as an unknown data point at WARN, invisible to the ERROR-based alerts. That
  one and every other downstream error — a 5xx, any other 4xx (a request amx-service built
  wrongly is not the controller's fault), a body that is not the `Errors` contract, connection
  refused — becomes 502, and the timeout 504. The downstream status is added to the details, so
  the log line carries it even when the body is unreadable.
  `ConfigurationCallExceptionProcessor` answers with that status (4xx logged at WARN, 5xx at
  ERROR). Until HAS-150 it answered a blanket 400 and logged everything at ERROR, so an
  upstream outage looked like a malformed request.
- **What a failing `database-service` says goes to the log, never into the answer.** Its
  `details` are raw exception text (SQL included) and `POST /home/amx` is reachable from
  outside. `ConfigurationCallException` therefore carries two sets: `errorMessages` (answered)
  and `downstreamMessages` (logged only, after `- downstream:`). Only the 404 with the code
  repeats the downstream message — a cause `database-service` chose to name.
- **Every way the lookup can go wrong ends as a `ConfigurationCallException`**, not only the
  failures up to the response headers: an answer without a body (a 2xx or a redirect — without
  `switchIfEmpty` the datagram was dropped and answered 200, with no log line), a connection
  lost in the middle of the body and a body that does not decode all become 502. The last
  `onErrorMap` catches whatever is not a `ConfigurationCallException` yet; it puts only the
  **type** of the failure into the details, because a decoding error quotes the body, and logs
  the exception itself at WARN (the ERROR line stays the processor's, one per failure). JSON
  that is no configuration counts as no answer too: `{}` decodes into an object of nulls —
  `@NotNull` on the SDK model is not enforced on decode — so the client checks `type` and
  `room` itself, or the failure would surface in `AmxService` as a 500.
- **Of a relayed 404 only the message carrying the code is answered**; any other message in the
  same body goes to the log like the body of a failure.
- **The code is a string owned by `database-service`** (`CustomErrorDescription`, pinned there by
  `CustomErrorDescriptionTest`; sent since its 0.8.0). `UNKNOWN_DATA_POINT_CODE` repeats it —
  nothing compiles against the other repo, so a rename on that side turns every unknown data
  point into a 502 here, and so does a `database-service` older than 0.8.0. Never deploy this
  service next to one.
- `DownstreamErrors` waits at most 2 s for an error body, counted from the response headers,
  while `response-timeout` counts from the start of the call. A stalled error body therefore
  keeps its status only when the headers arrived at least 2 s before the timeout; later than
  that — or with a `response-timeout` of 2 s or less — it ends as a 504. Accepted: closing it
  would take a per-call body timeout from the library.
- The `WebClient` is built from the **autoconfigured** `WebClient.Builder` (`AppConfig`) — an own
  builder bean would not be instrumented and every outgoing call would drop the trace.

## RabbitMQ

- **No own `RabbitTemplate` bean** (`RabbitConfig`): `RabbitAutoConfiguration` backs off from
  any `RabbitOperations` bean, and only the auto-configured template gets
  `spring.rabbitmq.template.*` — `observation-enabled` (the `traceparent` that carries the trace
  into `heating-service`) and the `retry` block. The message converter bean is picked up by the
  configurer.
- `TemperaturePublisher` wraps the blocking `convertAndSend` in `Mono.fromRunnable` on
  `Schedulers.boundedElastic()` — the one deliberate blocking call on this service's path.

## Build, tests & gotchas

- `mvn verify` (JDK 21). `tidy-maven-plugin:check` runs in `verify` — after editing the pom run
  `mvn tidy:pom`. Mockito runs as an explicit `-javaagent` with **`@{argLine}` first** so JaCoCo's
  agent from the Sonar workflow survives.
- Tests: `DeviceDatabaseClientTest` on `MockWebServer` — assert the **status carried by the
  exception**, and `AmxControllerTest` (`@WebFluxTest` + `@Import(ExceptionHandlerConfig.class)`)
  for the **HTTP status the AMX controller actually gets**; a client-level assertion alone once
  passed while the response was still a 400. `AmxConfigurationLookupTest` joins the two: the
  controller, the real service and client against a `MockWebServer`, so what `database-service`
  answers is checked against the status of the response **and the level of the log line** — the
  alerts look at ERROR, and that level is the point of HAS-176 (a logback `ListAppender` on the
  processor's logger, so it holds for plain and JSON logs alike). The server there lives as long
  as the class, so it answers through a `Dispatcher` set per test, not from the queue: a
  response one test left unread would be served to the next (it points the client at the stub with
  `@DynamicPropertySource` — the application class already registers
  `DeviceDatabaseClientConfig`, a second bean of that type breaks the context). Any short timeout belongs in the single test that
  needs it: the first call of a fresh `WebClient` initialises Netty, which on the CI runner with
  JaCoCo can take longer than a few hundred milliseconds.

## CI/CD

`CI.yml`, `sonar.yml` (SonarCloud + JaCoCo), `release.yml` (GitHub release → Docker image
`magikabdul/amx-service`). Manifest: `deployment-tools/workshop/amx-service.yaml` (image tag
pinned there). Release flow: the `release` skill.

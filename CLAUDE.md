# amx-service

`cloud.cholewa:amx-service` — the bridge between the AMX/NetLinx control system and the backend:
the AMX master forwards every frame it receives from the Eaton wireless gateways over a TCP
socket, this service parses it, finds out which device the data point is and publishes the
result on RabbitMQ — a room temperature for `heating-service`, what an actuator reports as a
`DeviceEvent` for the services of the devices; commands for Eaton blinds and lights go back
over the same socket as raw frames. Reactive (WebFlux), Java 21, Spring Boot 4.1.1 (`spring-boot-starter-parent`), Maven.
Local port **6001** (management **8001**); in the deployed `home` profile **6200** with Actuator
on **8200**. Docker image `magikabdul/amx-service`; the pom keeps `0.0.1-SNAPSHOT`, the released
version comes from the git tag. Not to be confused with `amx` (the NetLinx sources of the
controller itself, on the personal account) or with `api-gateway-service` (the HTTP edge).

Org-wide conventions and working rules (PR flow, branch naming `feature/HAS-<n>`, "user writes
service code, Claude reviews", both reviews before merge, own libraries on their latest
release) live in the workspace `organization.md` — this file only covers what is specific to
this repo. When opened as part of the workspace, those rules apply here too.

## Flow

**The socket is the only way between the AMX and this service** (HAS-286; until 1.4.0 the
master posted the frames to `POST /home/amx`, which is gone). `FrameServerClient` keeps one
permanent connection to the frame server of the Television master (`Gateway_SpringBoot_REST_nl3_0_0`,
HAS-230, port 20010; **the line protocol is pinned in the `CLAUDE.md` of the `amx` repository**,
section "The line protocol of the socket" — a change there is a change here). Every frame line
`<blinds|lights>;<hex>` the master sends becomes a `ReceivedFrame` → `EatonFrameReceiver` →
`AmxService`:

1. the type byte first: a `C9` error frame (32 bytes of text, which the validator does not
   know) is a WARN and done; after the validator, a `C3` status frame — the gateway's answer to
   a command, this service's or the Bedroom master's; it carries no data point, `1C` would be
   looked up as point 28 — is read: `1C 04` (`STT_OK`, `STS_OK_MRF`) is the telegram sent, an
   INFO; `09 xx` (`STT_ERROR`) is a telegram the gateway did **not** send, a WARN with the
   reason (unknown command, point out of range, RF busy — the caller of the command already got
   its 202, the log line is all that tells it); any other status type an INFO;
2. validate and split the frame (`eaton/utils` — `MessageValidator`, `MessageUtilities`);
3. look the data point up in `database-service` (`DeviceDatabaseClient`,
   `GET /home/device/configuration/eaton?point=&gateway=`) — roughly every 30 s, day and night;
4. by the configured device type: `TEMPERATURE_SENSOR` → `TemperaturePublisher` publishes a
   `TemperatureMessage` to the fanout exchange `temperature.events` (vhost `/temperature`),
   consumed by `heating-service` alone: `temperature.prod.heating` in the cluster, and
   `temperature.dev.heating` — bound to the same exchange, so it gets a copy of every
   reading — when that service runs locally. `BLINDS`, `LIGHT` and `DIMMER` (HAS-229) →
   `DeviceEventParser` reads the RX frame into a `DeviceEvent` of `smart-home-sdk` and
   `DeviceEventPublisher` publishes it to `amq.topic` of the vhost `/device` with the routing
   key `eaton.<gateway>.<point>` (below, "Device events"). `OTHER` is a WARN and the frame is
   dropped — `database-service` never answers it today (a kind without an old type is a 404).

The other direction: `POST /home/amx/eaton/blinds/{point}`, `.../blinds/{point}/status` (the
status request `0B` of HAS-291, for `blinds-service`), `.../lights/{point}`,
`.../lights/{point}/level` (`EatonCommandController`) → `EatonCommandService` →
`EatonDeviceRegistry` (which gateway) → `EatonFrameBuilder` → `FrameServerClient`. 202 with
`sentAt` once the server said `OK`. The contract for callers is in the README.

## The socket — read before touching `FrameServerClient` or `EatonFrameReceiver`

- **The client connects at the start and again, with a backoff, whenever the connection
  ends** (`SmartLifecycle`; 1 s to 30 s between attempts, `RECONNECT_MIN` / `RECONNECT_MAX`):
  the connection is the only way to receive. A master that is switched off is announced once
  at WARN, every further attempt is a DEBUG line, the connection an INFO. The master takes one
  client and reopens its port a second after a disconnect, so the first attempt after a drop
  may fail — expected. Its lifecycle phase is `DEFAULT_PHASE - 4096`: it starts before and
  stops **after** the web server and its graceful shutdown, so a command served while the pod
  goes down still finds the socket (1.4.0 had `@PreDestroy`, which came after the web server
  too).
- **A connection nothing arrives on for five minutes is taken for dead** (`READ_IDLE_LIMIT`, a
  `ReadTimeoutHandler` in front of the line decoder, plus `SO_KEEPALIVE`): a master that was
  power-cycled, or an address translation on the way that forgot the connection, sends no FIN,
  and the client would otherwise sit on an open-looking socket receiving nothing until a
  command happened to time out — 1.4.0 replaced an idle connection every two minutes for the
  same reason. The sensors report every few seconds between them, so five minutes of silence is
  a dead connection or gateways that are off; either way a reconnect costs nothing. The log
  line says why a connection ended (`Link.whyEnded`).
- **The lines are told apart as the protocol says** (`received`): a line with the prefix
  `blinds;` or `lights;` is a frame and goes to `frames()`; `OK`, `ERR` or `ERR <reason>` is the
  answer of the command in flight — or a WARN when none is; an empty line is ignored; anything
  else is logged and ignored and **never taken for an answer** (1.4.0 closed the connection on
  such a line; now a command whose answer is not recognised simply times out).
- **`frames()` is a unicast sink with a buffer of 256**: one subscriber (`EatonFrameReceiver`,
  subscribed in `@PostConstruct`, before the lifecycle connects), frames beyond the buffer are
  dropped with a WARN. A `ReceivedFrame` carries **the time the line arrived** (the `Clock`
  bean) and its text made printable once — every later log line of the frame is safe, and the
  `TemperatureMessage` is dated with the arrival, not the publishing: a backlog behind a lookup
  that failed for a while is published as the old news it is. The receiver processes **one
  frame at a time** (`concatMap`; the house produces a frame every ~30 s, a 5 s lookup timeout
  cannot build a backlog, and parallel lookups during an outage would only multiply the load on
  `database-service`'s pool of four) and never ends: `handle` lets no failure out, not even
  one of the explaining of a failure. It runs each frame under an **observation of its own**
  (`eaton.frame`, put into the reactor context as `ObservationThreadLocalAccessor.KEY`), which
  is what gives the lines of one frame a shared `traceId` — a frame has no request to take one
  from. The "Received frame" line is written inside the chain, before the pipeline is
  subscribed to; the pipeline writes its first lines on the subscribe itself.
- **The level of the receiver's log line is what the alerts see**, pinned by
  `EatonFrameReceiverTest` and `EatonFrameLookupTest` (the real `DeviceDatabaseClient` against a
  `MockWebServer`): parser refusals and a 404 with the code `NOT_FOUND_DEVICE_CONFIGURATION` are
  WARN, every other `ConfigurationCallException` (502, 504) and anything unexpected ERROR. What
  a failing `database-service` said is in that line after `downstream:`
  (`ConfigurationCallException.describe()`) and nowhere else — there is no HTTP answer it could
  leak into any more.
- **The command rules of 1.4.0 still hold, each with a test**: one command at a time
  (`concatMap` on the queue of commands); a frame is never sent twice; a command whose caller
  gave up is not sent; the outcome of a command is the first `OK` / `ERR` line after it, within
  `response-timeout`; **a timeout or a lost connection closes the connection** — the late
  answer would be read as the answer of the next command — and the loop opens another; a
  command while the socket is not open is refused at once (`FRAME_SERVER_UNAVAILABLE`, "not
  connected; the frame was not sent"). Gone: the idle replacement of a connection and the
  three attempts to connect per command — the loop owns the connection.
- **`Link.ended()` is `connection.onDispose()`**, not the end of the inbound stream: a
  connection this client disposes ends its inbound with a cancel, and the loop would never
  learn of it.
- **Two things only `doFinally` taught** (1.4.0): it runs *after* the next command has started,
  so the sink a command waits on is cleared with `compareAndSet`, never set to `null`. And a
  context needs `@Autowired` on the public constructor, the class having a second one for tests.
- **What the client cannot know**: a connection the master dropped without a word looks open
  until a command on it stays unanswered (that one is a 504) or the master closes it properly.
  Accepted.
- **Health**: `FrameServerHealthIndicator` (`frameServer`) is `DOWN` while the socket is not
  open, and **on purpose outside the readiness and liveness groups** (the default groups hold
  the application states alone) — a master that is off must not take the pod out of the Service
  or restart it; `FrameServerHealthTest` pins both. The whole `/actuator/health` is then 503,
  which nothing in the cluster reads.
- **`amx.frame-server.host` has no default in the shared document** — `AMX_FRAME_SERVER_HOST` in
  the manifest; `local` and `test` point at `localhost` (`test`: port 1, so every context test
  runs the reconnect loop against a closed port — one WARN, then DEBUG). The context tests carry
  `@ActiveProfiles("test")` for that reason.
- **The frames are the documented TX packet, length `06`** (`EatonFrameBuilder`:
  `5A,06,B1,<point>,<event>,<data>,00,A5`; owner's decision of 2026-10-14, verified on the
  Boiler blind and light through the frame server). The `09` form with four bytes of padding
  is what the NetLinx module behind the panels still sends; the gateways accept both.
- **The frame server writes every byte in two digits** (`03`), the HTTP forwarder wrote none
  that were not needed (`3`): anything that compares a byte of a frame compares numbers
  (`TemperatureParser.byteOf`), never strings.

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
  wrongly is not the master's fault), a body that is not the `Errors` contract, connection
  refused — becomes 502, and the timeout 504. The downstream status is added to the details, so
  the log line carries it even when the body is unreadable. For a received frame the status
  decides the level of the receiver's line (above); for a command, `EatonDeviceRegistry` fails
  the same way and `ConfigurationCallExceptionProcessor` answers with that status (4xx logged
  at WARN, 5xx at ERROR).
- **`ConfigurationCallException` carries two sets**: `errorMessages` (the cause as this
  service names it, answered to a caller of a command — `cause()`) and `downstreamMessages`
  (what `database-service` said — raw exception text, SQL included — logged only, after
  `downstream:` — `describe()`, the one line both the receiver and the processor log). Only
  the 404 with the code repeats the downstream message — a cause `database-service` chose to
  name. An `ErrorMessage` without details or message is left out, never a throw: the receiver
  would end on it.
- **Every way the lookup can go wrong ends as a `ConfigurationCallException`**, not only the
  failures up to the response headers: an answer without a body (a 2xx or a redirect — without
  `switchIfEmpty` the frame was dropped with no log line), a connection lost in the middle of
  the body and a body that does not decode all become 502. The last `onErrorMap` catches
  whatever is not a `ConfigurationCallException` yet; it puts only the **type** of the failure
  into the details, because a decoding error quotes the body, and logs the exception itself at
  WARN (the ERROR line stays the receiver's or the processor's, one per failure). JSON that is
  no configuration counts as no answer too: `{}` decodes into an object of nulls — `@NotNull`
  on the SDK model is not enforced on decode — so the client checks `type` and `room` itself.
- **Of a relayed 404 only the message carrying the code is answered**; any other message in the
  same body goes to the log like the body of a failure.
- **The code is a string owned by `database-service`** (`CustomErrorDescription`, pinned there by
  `CustomErrorDescriptionTest`; sent since its 0.8.0). `UNKNOWN_DATA_POINT_CODE` repeats it —
  nothing compiles against the other repo, so a rename on that side turns every unknown data
  point into an ERROR here, and so does a `database-service` older than 0.8.0. Never deploy this
  service next to one.
- `DownstreamErrors` waits at most 2 s for an error body, counted from the response headers,
  while `response-timeout` counts from the start of the call. A stalled error body therefore
  keeps its status only when the headers arrived at least 2 s before the timeout; later than
  that — or with a `response-timeout` of 2 s or less — it ends as a 504. Accepted: closing it
  would take a per-call body timeout from the library.
- The `WebClient` is built from the **autoconfigured** `WebClient.Builder` (`AppConfig`) — an own
  builder bean would not be instrumented and every outgoing call would drop the trace.

## Eaton commands — read before touching `EatonCommandService` or `EatonDeviceRegistry`

- **The log lines of the client carry the `traceId` of the request** although they run on the
  subscriber of the queue: `send` captures the reactor context and `carryOut` writes it back.
- **A kept list of the registry is believed only when it says yes** (`EatonDeviceRegistry`): one
  device on the point. Before a refusal — 404, 409, and `NOT_A_DIMMER` in the service
  (`findAgain`) — the registry is read again, unless the list is younger than ten seconds;
  callers at the same moment share one read. A removed or moved device is still driven the old
  way for five minutes: that is the cache the task asked for.
- **This service is a reader of the device registry** (`DeviceRegistryClient`, lists of
  `SmartDevice` by kind). A new `RoomName`, `LightKind`, `BlindsPlacement` or `SmartVendor`
  stored on a device of the kind `blinds` or `light` before this service has the SDK that knows
  it makes the list undecodable — every command of that kind a 502.
- **The codes are wire contract** (`CustomError`, pinned by `CustomErrorTest`): `NOT_FOUND_DEVICE`,
  `AMBIGUOUS_POINT`, `NOT_A_DIMMER`, `FRAME_SERVER_UNAVAILABLE`, `FRAME_SERVER_TIMEOUT`,
  `FRAME_REFUSED`. `EatonCommandException`'s message is answered as `details` — put nothing in
  it a response must not carry (the address of the master goes to the log line only).
- **Copies to recognise** (the rule of the organization: into `cholewa-commons` at the fourth):
  `InvalidRequestParameterProcessor` is the second copy, after `database-service`. And
  `DeviceRegistryClient` repeats the failure chain of `DeviceDatabaseClient` — a fix to one
  belongs in both.

## Device events — read before touching `DeviceEventParser` or `DeviceEventPublisher`

- **The event names the device the way the datagram does** — vendor `eaton`, the gateway, the
  point — never by the registry: the consumer looks the device up in the registry itself. The
  lookup here only decides *whether* a frame is published: an Eaton point the registry does not
  hold as blinds, light, dimmer, push button or motion sensor is not. The last two (HAS-288,
  `smart-home-sdk` 1.9.0) only report - a wall push button `push button on / off`, a PIR sensor
  `switch on / off` - and are never sent anything: the writes look the device up by the kind of
  the command (`blinds`, `light`), so a push button cannot be driven by mistake. **This service
  has to run 1.9.0 before `database-service` answers either type** - on an older SDK the lookup
  of such a point does not deserialize, and every one of its frames is an ERROR.
- **The gateway's error statuses (`C3`, type `09`) are worded by what they mean**: `01`–`04`
  the telegram was not sent; `06` (`statusNoAcknowledgement` of the NetLinx module) it went out
  and no actuator confirmed it - the point has no actuator, or one out of range or without
  power, seen on the dismantled lights of the inventory of 2026-10-15; `05` and anything newer
  by number only. All WARN: the caller already has its 202.
- **The RX layout is `moe_protocol.h`** (the `CLAUDE.md` of `amx` has it): after
  `extractMessage` the indexes are length 0, type 1 (`C1`), point 2, message type 3, data type 4,
  info short 5, data 6–9 (high byte first), RSSI 10, battery 11. Only a 12-byte `C1` frame is an
  event.
- **`value` only for `status` and `analog value`** (the SDK says so): the status of an actuator
  is its **info short** — blinds `0` stop / `1` open / `2` close, a switch `0` / `1`, a dimmer
  its level 0–100; an analog value is read by its data type, only when it is a whole number —
  percent (`01`, the byte 0–255 published as 0–100) or unsigned short (`0D`). A temperature,
  a float or room-controller data is refused (WARN). A message type without an event (`63`,
  `64`, `80`, …) is refused too, never published under another name.
- **`RxEventType` and `EatonRxEvent` share names, not order**: mapped by name, every constant
  pinned by `DeviceEventParserTest`.
- **A second connection, never a bean** (`DeviceEventConnection`, built in
  `DeviceEventsRabbitConfig`): a connection belongs to one vhost and the one of
  `spring.rabbitmq.*` leads to `/temperature`; any `ConnectionFactory` or `RabbitOperations`
  bean would make `RabbitAutoConfiguration` back off and take the observation and retry of the
  temperature publisher with it (`DeviceEventConnectionTest` counts the beans). It is the shape
  of `NotificationConnection` of `cholewa-commons` — a copy on purpose: that class names its
  connection `/notification` and words its returns as notifications. The broker shows it as
  `<pod>/device`; user `device`, the password `device-events.password` has no default
  (`DEVICE_EVENTS_PASSWORD` in the manifest, secret `rabbitmq`, key `device-password`) — a pod
  without it does not start, and a local run needs it in the env file.
- **A publish is done when the broker confirmed it and did not hand it back** (correlated
  confirms, mandatory returns), within `device-events.confirm-timeout` (5 s, bounded 1–10 s) —
  **the send and the confirmation together**: a send stuck on opening a connection (10 s of
  `spring.rabbitmq.connection-timeout`) would otherwise hold the frames twice as long. Anything
  else is a `DeviceEventPublishException` and **the ERROR line of the receiver**, with the
  cause; nothing is retried — the next frame of the device says its state again (a send cut off
  by the timeout may still arrive: late, never twice). The frames are processed one at a time,
  so a broker that does not answer holds the next frame up to the timeout. The confirmation arrives on the thread of the connection: the chain moves to
  `boundedElastic` before anything else runs (the lesson of `NotificationPublisher`).
- **Every `device.<env>.<domain>` queue is bound to `amq.topic` with `#`**, so every event
  reaches the `prod` and the `dev` queues of every domain, and a local run that publishes
  reaches the queues of production too. A routing key with no queue at all is a returned
  message — an ERROR, not a quiet loss.
- The JSON is the converter of the temperature messages (`JacksonJsonMessageConverter`):
  `{"at":"2026-10-15T18:30:05","vendor":"eaton","gateway":"blinds","point":14,"event":"status","value":1}`,
  `at` the arrival of the frame, no `value` where the event carries none — pinned by
  `DeviceEventConnectionTest`.

## RabbitMQ

- **No own `RabbitTemplate` bean** (`RabbitConfig`): `RabbitAutoConfiguration` backs off from
  any `RabbitOperations` bean, and only the auto-configured template gets
  `spring.rabbitmq.template.*` — `observation-enabled` (the `traceparent` that carries the trace
  into `heating-service`) and the `retry` block. The message converter bean is picked up by the
  configurer.
- `TemperaturePublisher` wraps the blocking `convertAndSend` in `Mono.fromRunnable` on
  `Schedulers.boundedElastic()`, and `DeviceEventPublisher` its send the same way — the
  deliberate blocking calls on this service's path.

## Build, tests & gotchas

- `mvn verify` (JDK 21). `tidy-maven-plugin:check` runs in `verify` — after editing the pom run
  `mvn tidy:pom`. Mockito runs as an explicit `-javaagent` with **`@{argLine}` first** so JaCoCo's
  agent from the Sonar workflow survives.
- `FrameServerClientTest` runs against `FrameServerStub`, a plain socket server in the test
  sources: lines in, one line out, `push` for the frames the master sends unasked, any number of
  clients so the connections can be counted — **count them relative to what `setUp` opened**,
  the client connects by itself. A short response timeout belongs to the tests of a silent
  server alone; the reconnect pauses are short in the tests (constructor for tests).
- Tests: `DeviceDatabaseClientTest` on `MockWebServer` — assert the **status carried by the
  exception**; `EatonFrameLookupTest` joins the receiver, the real service and client against a
  `MockWebServer`, so what `database-service` answers is checked against **the level of the log
  line** — the alerts look at ERROR, and that level is the point of HAS-176 (a logback
  `ListAppender` on the receiver's logger, so it holds for plain and JSON logs alike).
  `EatonCommandControllerTest` (`@WebFluxTest` + `@Import(ExceptionHandlerConfig.class)`) does
  the same for the **HTTP status a caller of a command actually gets**. Any short timeout
  belongs in the single test that needs it: the first call of a fresh `WebClient` initialises
  Netty, which on the CI runner with JaCoCo can take longer than a few hundred milliseconds.
- **A `MockWebServer` per test never gets a fixed port, and its client no connection pool**
  (HAS-216). The clients `WebClient.create()` builds share one pool, keyed by host and port.
  `DeviceDatabaseClientTest` used to start each test's server on port 3000: a test could then
  be handed the connection the test before it had left in the pool — to a server already
  closed — and fail as "database-service unreachable", because Reactor Netty does not repeat
  a request that ends as "prematurely closed before response". It happened rarely: once in
  about 7900 rounds of a probe, and once in a build on `main`. The server now takes whatever
  port the system gives and the client is built on `HttpClient.newConnection()`, so nothing
  is inherited.
- `FrameServerHealthTest` starts the application on random ports and reads the Actuator with a
  plain `WebClient` (`@SpringBootTest` registers no `WebTestClient` here); the broker's health
  indicator is switched off in it, or the whole health would wait out the broker's connection
  timeout. The `@SpringBootTest` contexts keep the reconnect loop running against
  `localhost:1` for as long as they are cached — harmless.
- **The RabbitMQ connection is named after the pod** (HAS-106): the broker shows
  `amx-service-<pod id>`, which tells the old pod from the new one during a rollout.
  `HOSTNAME` is taken only when it starts with `spring.application.name` and a dash — outside
  the cluster it is missing, empty, a workstation or a container id, and the name is then
  `amx-service-local`. **Since HAS-206 the rule is not code of this service**: the strategy is
  the bean `cholewaPodConnectionNameStrategy` of `cholewa-commons` (≥ 1.8.0,
  `RabbitConnectionNameAutoConfiguration`), handed by Boot to the auto-configured connection
  factory. It backs off from any `ConnectionNameStrategy` bean declared here, so do not add
  one; `RabbitConnectionNameTest` starts the whole context (`HOSTNAME` set by the test, no
  connection opened) and fails when the library's strategy is not the one the factory uses or
  the name changes.
- **A local proof of the socket**: run the jar with `home,local` and a loopback stand-in of the
  frame server on 20010 (a few lines of PowerShell with a `TcpListener` that pushes a frame line
  and answers `OK`); the log shows "Connected", the frame with its own `traceId`, and — without
  a local `database-service` — the lookup failing at ERROR. The commands can be sent with Bruno
  (`amx/eaton`).

## CI/CD

`CI.yml`, `sonar.yml` (SonarCloud + JaCoCo), `release.yml` (GitHub release → Docker image
`magikabdul/amx-service`). Manifest: `deployment-tools/workshop/amx-service.yaml` (image tag
and `AMX_FRAME_SERVER_HOST` pinned there). Release flow: the `release` skill.

# tcron

`tcron` is a Go package for registering and executing background jobs on top of [robfig/cron/v3](https://github.com/robfig/cron). It supports recurring jobs, one-shot jobs, optional schedule jitter, and Redis-backed singleton execution across processes. When used with [ttrace](https://github.com/choveylee/ttrace) and [tmetric](https://github.com/choveylee/tmetric), each execution also emits an OpenTelemetry span and a latency histogram.

## Requirements

- Go 1.27.4 or later (see `go.mod`).

## Features

- **Recurring scheduling** — Register a handler with a cron expression that includes the seconds field.
- **Schedule jitter** — Add a uniform random delay bounded by `delta` and the interval to the following tick.
- **One-shot execution** — Unregister a job as soon as its first execution is admitted, before calling the handler.
- **Distributed singleton execution** — Use a Redis lease with an ownership token and compare-and-delete release. The lease protects the job until it expires.
- **Observability** — Emit a span for each execution (via [ttrace](https://github.com/choveylee/ttrace)) and record a latency histogram labeled by job name and status.

## Installation

```bash
go get github.com/choveylee/tcron
```

## Usage

### Recurring job

```go
id, err := tcron.RegisterCron("0 */5 * * * *", myJob, 0) // every 5 minutes, no jitter
```

### Jitter

If `delta` is positive, each scheduled activation adds a random offset in `[0, delta)`, shortened when necessary to land before the following scheduled tick. Registration returns an error when the scheduler can find no next execution time.

```go
id, err := tcron.RegisterCron("0 * * * * *", myJob, 30*time.Second, arg1, arg2)
```

### One-shot job

```go
id, err := tcron.RegisterOnceCron("0 * * * * *", myJob)
```

### Singleton job (Redis)

Pass a Redis client implementing `tcron.CronRedisClient` and supporting Redis `EVAL` (for example `*redis.Client`, `*redis.ClusterClient`, `*redis.Ring`, or `redis.UniversalClient`), together with a lease TTL. Set the TTL to cover acquisition and the full handler run. Other processes can acquire the lease after expiry. If `lockTtl <= 0`, the library uses the first scheduled interval, or one minute when no interval is available. The effective TTL uses Redis millisecond precision with a minimum of one millisecond.

Acquisition and release use contexts with deadlines of up to five seconds. Enable `ContextTimeoutEnabled` on go-redis clients to apply these deadlines to Redis I/O. An acquisition that finishes past its deadline is skipped. Passing a nil Redis client or a client without `Eval` support causes registration to fail.

```go
id, err := tcron.RegisterSingletonCron("0 * * * * *", myJob, rdb, 5*time.Minute)
```

### Singleton one-shot

```go
id, err := tcron.RegisterSingletonOnceCron("0 * * * * *", myJob, rdb, 5*time.Minute, arg1)
```

A tick that cannot acquire the lease can retry on a later tick. Once execution is admitted, that registration is consumed. Each process has its own registration, so a different process may execute its one-shot handler at a later tick.

### Unregister

```go
ok := tcron.RemoveCron(id)
```

Removal prevents later admissions, including an invocation still waiting to acquire its Redis lease. A handler that was already admitted may finish. Recurring handlers may overlap and each invocation receives its own argument slice. Use synchronization for mutable objects passed through that slice.

## Job identity and Redis keys

Job names come from the handler's runtime function name. Singleton jobs use that name as the Redis lease key. Each acquisition uses a random token and releases only the matching key. Closures typically receive synthetic names (for example `main.main.func1`). Use top-level functions when lock keys and metric labels need to be stable across builds.

## Metrics

The package registers a histogram named `cron_job_latency` (help text: `CronJob`) with the labels `name` and `status` (`success` or `failed`). Recorded values are latencies in milliseconds, consistent with the default bucket configuration provided by [tmetric](https://github.com/choveylee/tmetric). If histogram registration fails, the package logs a warning and continues with latency metric collection disabled.

# tcron

`tcron` is a Go package for registering and executing background jobs on top of [robfig/cron/v3](https://github.com/robfig/cron). It supports recurring jobs, one-shot jobs, optional schedule jitter, and Redis-backed singleton execution across processes. When used with [ttrace](https://github.com/choveylee/ttrace) and [tmetric](https://github.com/choveylee/tmetric), each execution also emits an OpenTelemetry span and a latency histogram.

## Requirements

- Go 1.25 or later (see `go.mod`).

## Features

- **Recurring scheduling** — Register a handler with a cron expression that includes the seconds field.
- **Schedule jitter** — Add a uniform random delay in `[0, delta)` to each computed next run time to reduce thundering herds.
- **One-shot execution** — Schedule a job for the next matching tick and automatically unregister it after the first execution attempt.
- **Distributed singleton execution** — Ensure that at most one process executes a job at a time by using a Redis lock with token-based compare-and-delete release.
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

If `delta` is positive, each scheduled activation time is the next time computed by the cron engine plus a random offset in `[0, delta)`.

```go
id, err := tcron.RegisterCron("0 * * * * *", myJob, 30*time.Second, arg1, arg2)
```

### One-shot job

```go
id, err := tcron.RegisterOnceCron("0 * * * * *", myJob)
```

### Singleton job (Redis)

Pass a Redis client implementing `tcron.CronRedisClient` and supporting Redis `EVAL` (for example `*redis.Client`, `*redis.ClusterClient`, `*redis.Ring`, or `redis.UniversalClient`), together with a lock TTL. The TTL should exceed the worst-case execution time of the handler. If `lockTtl <= 0`, the library uses the interval between consecutive schedule ticks; if that interval is zero, it uses one minute. Passing a nil Redis client, or a client without `Eval` support, causes registration to fail.

```go
id, err := tcron.RegisterSingletonCron("0 * * * * *", myJob, rdb, 5*time.Minute)
```

### Singleton one-shot

```go
id, err := tcron.RegisterSingletonOnceCron("0 * * * * *", myJob, rdb, 5*time.Minute, arg1)
```

### Unregister

```go
ok := tcron.RemoveCron(id)
```

## Job identity and Redis keys

Job names are derived from `runtime.FuncForPC(reflect.ValueOf(f).Pointer()).Name()`. For singleton jobs, this string becomes the Redis lock key, while the lock value is a unique token so release can use compare-and-delete rather than unconditionally deleting the key. Closures typically receive synthetic names (for example `main.main.func1`); for stable lock keys and metric labels, prefer top-level functions or document the naming implications for your deployment.

## Metrics

The package registers a histogram named `cron_job_latency` (help text: `CronJob`) with the labels `name` and `status` (`success` or `failed`). Recorded values are latencies in milliseconds, consistent with the default bucket configuration provided by [tmetric](https://github.com/choveylee/tmetric). If histogram registration fails, the package logs a warning and continues with latency metric collection disabled.

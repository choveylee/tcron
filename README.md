# tcron

`tcron` is a small Go library that wraps [robfig/cron/v3](https://github.com/robfig/cron) to register periodic and one-shot jobs with optional schedule jitter, optional distributed mutual exclusion using Redis, OpenTelemetry tracing, and Prometheus-style latency histograms via [tmetric](https://github.com/choveylee/tmetric).

## Requirements

- Go 1.25 or later (see `go.mod`).

## Features

- **Standard cron jobs** — Register a handler with a cron expression (with seconds field).
- **Jitter** — Add a uniform random delay in `[0, delta)` to each computed next run time to reduce thundering herds.
- **Run once** — Schedule a job for the next tick only, then unregister it automatically.
- **Singleton (distributed lock)** — Ensure at most one instance runs the job at a given time across processes, using Redis `SET NX` keyed by the handler’s reflected name.
- **Observability** — Spans for each run (via [ttrace](https://github.com/choveylee/ttrace)) and a latency histogram labeled by job name and status.

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

If `delta` is positive, each scheduled activation time is the cron engine’s next time plus a random offset in `[0, delta)`.

```go
id, err := tcron.RegisterCron("0 * * * * *", myJob, 30*time.Second, arg1, arg2)
```

### One-shot job

```go
id, err := tcron.RegisterOnceCron("0 * * * * *", myJob)
```

### Singleton job (Redis)

Pass a Redis client implementing `tcron.CronRedisClient` and a lock TTL. The TTL should exceed the worst-case execution time of the handler. If `lockTtl <= 0`, the library uses the interval between consecutive schedule ticks; if that is zero, it uses one minute.

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

Job names are derived from `runtime.FuncForPC(reflect.ValueOf(f).Pointer()).Name()`. For singleton jobs, this string is the Redis lock key. Closures typically receive synthetic names (for example `main.main.func1`); for stable keys and metrics, prefer top-level functions or document naming limitations for your deployment.

## Metrics

The package registers a histogram `cron_job_latency` (help: `CronJob`) with labels `name` and `status` (`success` or `failed`). Values are latencies in milliseconds, consistent with [tmetric](https://github.com/choveylee/tmetric) defaults.

// Package tcron schedules recurring, one-shot, and Redis-backed jobs with tracing and metrics.
// Its process-wide scheduler uses the local time zone unless a schedule supplies its own.
package tcron

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	mathrand "math/rand/v2"
	"reflect"
	"runtime"
	"runtime/debug"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/choveylee/tlog"
	"github.com/choveylee/tmetric"
	"github.com/choveylee/ttrace"
	"github.com/choveylee/tutil"
	"github.com/redis/go-redis/v9"
	"github.com/robfig/cron/v3"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	oteltrace "go.opentelemetry.io/otel/trace"
)

var defaultCronManager = &cronManager{
	cronClient: cron.New(cron.WithLocation(time.Local)),
	cronParser: cron.NewParser(cron.Second | cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow | cron.Descriptor),

	jobByIdMap: make(map[string]*cronJob),
}

var (
	_ cron.Job      = (*cronJob)(nil)
	_ cron.Schedule = (*deltaSchedule)(nil)
)

const (
	redisCommandTimeout        = 5 * time.Second
	releaseSingletonLockScript = `
if redis.call("get", KEYS[1]) == ARGV[1] then
	return redis.call("del", KEYS[1])
end
return 0
`
)

func init() {
	defaultCronManager.cronClient.Start()
}

// CronRedisClient defines the Redis client contract used by singleton jobs.
// Implementations must also support Eval so the lock can be released only by its owner.
// Del remains part of the interface for compatibility with existing callers.
type CronRedisClient interface {
	SetNX(ctx context.Context, key string, value any, expiration time.Duration) *redis.BoolCmd
	Del(ctx context.Context, keys ...string) *redis.IntCmd
}

type cronRedisEvalClient interface {
	Eval(ctx context.Context, script string, keys []string, args ...any) *redis.Cmd
}

// RegisterCron registers a recurring handler using a cron expression with seconds.
// Positive delta adds random delay, bounded by the interval to the next scheduled tick.
// Each invocation receives its own argument slice. Referenced values remain shared.
func RegisterCron(spec string, handler func(ctx context.Context, params ...any) error, delta time.Duration, params ...any) (string, error) {
	return registerCron(spec, handler, false, false, delta, nil, 0, params...)
}

// RegisterOnceCron registers a handler for the next tick. It unregisters the job
// before invoking the handler, including when the handler subsequently fails or panics.
func RegisterOnceCron(spec string, handler func(ctx context.Context, params ...any) error, params ...any) (string, error) {
	return registerCron(spec, handler, false, true, 0, nil, 0, params...)
}

// RegisterSingletonCron registers a recurring job that acquires a Redis lease before
// invoking its handler. Configure lockTtl to cover acquisition and the full handler run.
// Other processes can acquire the lease after expiry. A non-positive TTL defaults to
// the first scheduled interval, or one minute when no interval is available.
func RegisterSingletonCron(spec string, handler func(ctx context.Context, params ...any) error, redisClient CronRedisClient, lockTtl time.Duration) (string, error) {
	return registerCron(spec, handler, true, false, 0, redisClient, lockTtl)
}

// RegisterSingletonOnceCron runs the handler at most once per registration.
// A tick that misses the Redis lease leaves the job registered for later ticks.
func RegisterSingletonOnceCron(spec string, handler func(ctx context.Context, params ...any) error, redisClient CronRedisClient, lockTtl time.Duration, params ...any) (string, error) {
	return registerCron(spec, handler, true, true, 0, redisClient, lockTtl, params...)
}

// RemoveCron unregisters the job and prevents pending invocations from entering its
// handler. Invocations already admitted may finish. It reports whether the job was found.
func RemoveCron(jobId string) bool {
	manager := defaultCronManager
	manager.Lock()
	defer manager.Unlock()

	job, ok := manager.jobByIdMap[jobId]
	if !ok {
		return false
	}

	job.registered.Store(false)
	delete(manager.jobByIdMap, jobId)
	manager.cronClient.Remove(job.entryId)

	return true
}

type cronManager struct {
	cronClient *cron.Cron
	cronParser cron.Parser

	jobByIdMap map[string]*cronJob

	sync.Mutex
}

// registerCron validates the input and publishes the job and its scheduler entry
// under one lock, so a fast one-shot callback can unregister an already published entry.
func registerCron(spec string, handler func(ctx context.Context, params ...any) error, singleton, once bool, delta time.Duration, redisClient CronRedisClient, lockTtl time.Duration, params ...any) (string, error) {
	if handler == nil {
		return "", errors.New("tcron: handler must not be nil")
	}

	var redisEval cronRedisEvalClient
	if singleton {
		if redisClient == nil {
			return "", errors.New("tcron: singleton Redis client must not be nil")
		}

		clientValue := reflect.ValueOf(redisClient)
		switch clientValue.Kind() {
		case reflect.Chan, reflect.Func, reflect.Map, reflect.Pointer, reflect.Slice:
			if clientValue.IsNil() {
				return "", errors.New("tcron: singleton Redis client must not be nil")
			}
		}

		var ok bool
		redisEval, ok = redisClient.(cronRedisEvalClient)
		if !ok {
			return "", errors.New("tcron: singleton Redis client must support Eval")
		}
	}

	spec = strings.TrimSpace(spec)
	if strings.HasPrefix(spec, "TZ=") || strings.HasPrefix(spec, "CRON_TZ=") {
		// cron/v3 requires a literal space after the time zone and panics when it is missing.
		fields := strings.Fields(spec)
		if len(fields) < 2 || strings.HasSuffix(fields[0], "=") {
			return "", errors.New("tcron: time zone prefix must specify a time zone and a schedule")
		}

		spec = strings.Join(fields, " ")
	}

	manager := defaultCronManager
	schedule, err := manager.cronParser.Parse(spec)
	if err != nil {
		return "", fmt.Errorf("tcron: parse schedule: %w", err)
	}

	next := schedule.Next(time.Now().In(manager.cronClient.Location()))
	if next.IsZero() {
		return "", errors.New("tcron: schedule must have a next execution time")
	}

	if singleton {
		if lockTtl <= 0 {
			following := schedule.Next(next)
			lockTtl = time.Minute
			if !following.IsZero() {
				lockTtl = following.Sub(next)
			}
		}

		// Match Redis millisecond precision so the local expiry check never overstates the lease.
		lockTtl = max(time.Millisecond, lockTtl.Truncate(time.Millisecond))
	}

	job := &cronJob{
		id:      tutil.NewOidHex(),
		name:    runtime.FuncForPC(reflect.ValueOf(handler).Pointer()).Name(),
		spec:    spec,
		handler: handler,
		args:    slices.Clone(params),

		once:        once,
		lockTtl:     lockTtl,
		redisClient: redisClient,
		redisEval:   redisEval,
	}

	if delta > 0 {
		schedule = &deltaSchedule{parent: schedule, delta: delta}
	}

	manager.Lock()
	defer manager.Unlock()

	job.registered.Store(true)
	job.entryId = manager.cronClient.Schedule(schedule, job)
	manager.jobByIdMap[job.id] = job

	return job.id, nil
}

type deltaSchedule struct {
	parent cron.Schedule
	delta  time.Duration
}

// Next delays a tick without moving it past its successor or changing a zero next time.
func (schedule *deltaSchedule) Next(now time.Time) time.Time {
	next := schedule.parent.Next(now)
	if next.IsZero() || schedule.delta <= 0 {
		return next
	}

	bound := schedule.delta
	following := schedule.parent.Next(next)
	if !following.IsZero() {
		bound = min(bound, following.Sub(next))
	}

	return next.Add(mathrand.N(bound))
}

type cronJob struct {
	id      string
	name    string
	spec    string
	handler func(ctx context.Context, params ...any) error
	args    []any

	once        bool
	lockTtl     time.Duration
	redisClient CronRedisClient
	redisEval   cronRedisEvalClient

	entryId    cron.EntryID
	registered atomic.Bool
	running    atomic.Bool
}

// Run checks registration again after lease acquisition. Once jobs atomically
// claim the registration by removing it. Redis contention leaves once jobs eligible to retry.
func (job *cronJob) Run() {
	if !job.registered.Load() {
		return
	}

	singleton := job.redisEval != nil
	if singleton {
		// Keep callbacks for the same local singleton from overlapping even after lease expiry.
		if !job.running.CompareAndSwap(false, true) {
			return
		}

		defer job.running.Store(false)
	}

	options := []oteltrace.SpanStartOption{
		oteltrace.WithSpanKind(oteltrace.SpanKindInternal),
		oteltrace.WithAttributes(
			attribute.String("cron.name", job.name),
			attribute.String("cron.spec", job.spec),
			attribute.Bool("cron.singleton", singleton),
			attribute.Bool("cron.once", job.once),
		),
	}

	ctx, span := ttrace.Start(context.Background(), "cron."+job.name, options...)
	defer span.End()

	var handlerStarted time.Time
	status := "failed"
	defer func() {
		if panicValue := recover(); panicValue != nil {
			status = "failed"
			span.SetStatus(codes.Error, "cron job panicked")
			tlog.E(ctx).Detailf("panic: %v, stack: %s", panicValue, debug.Stack()).Msg("Recover cron job panic")
		}

		if !handlerStarted.IsZero() && cronJobHistogram != nil {
			if err := cronJobHistogram.Observe(tmetric.SinceMS(handlerStarted), job.name, status); err != nil {
				tlog.W(ctx).Err(err).Msg("Record cron job latency error")
			}
		}
	}()

	var leaseStart time.Time
	if singleton {
		token := rand.Text()
		leaseStart = time.Now()
		acquireCtx, cancel := context.WithTimeout(ctx, min(redisCommandTimeout, job.lockTtl))
		defer cancel()

		acquired, err := job.redisClient.SetNX(acquireCtx, job.name, token, job.lockTtl).Result()
		acquireTimedOut := acquireCtx.Err() != nil
		cancel()

		if err != nil {
			span.SetStatus(codes.Error, "acquire Redis lease")
			tlog.W(ctx).Err(err).Msg("Acquire cron Redis lease error")

			return
		}

		if !acquired {
			return
		}

		defer releaseSingletonLock(ctx, job, token)
		if acquireTimedOut || time.Since(leaseStart) >= job.lockTtl {
			tlog.W(ctx).Msg("Skip cron job because the Redis lease was acquired too late")

			return
		}
	}

	if job.once {
		if !RemoveCron(job.id) {
			return
		}
	} else if !job.registered.Load() {
		return
	}

	if singleton && time.Since(leaseStart) >= job.lockTtl {
		tlog.W(ctx).Msg("Skip cron job because the Redis lease expired before execution")

		return
	}

	handlerStarted = time.Now()
	err := job.handler(ctx, slices.Clone(job.args)...)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "cron handler failed")
		tlog.E(ctx).Err(err).Msgf("Run cron job error (job id=%s, name=%s, elapsed=%s)", job.id, job.name, time.Since(handlerStarted))

		return
	}

	status = "success"
	tlog.I(ctx).Msgf("Run cron job complete (job id=%s, name=%s, elapsed=%s)", job.id, job.name, time.Since(handlerStarted))
}

// releaseSingletonLock uses a separate bounded context and deletes only the lease
// carrying this invocation's token. An expired lease may already belong to another worker.
func releaseSingletonLock(ctx context.Context, job *cronJob, lockToken string) {
	releaseCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), redisCommandTimeout)
	defer cancel()

	deleted, err := job.redisEval.Eval(releaseCtx, releaseSingletonLockScript, []string{job.name}, lockToken).Int64()
	if err != nil {
		tlog.W(ctx).Err(err).Msg("Release cron Redis lease error")

		return
	}

	if deleted == 0 {
		tlog.W(ctx).Msg("Skip cron Redis lease release because ownership changed")
	}
}

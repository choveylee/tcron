/**
 * @Author: lidonglin
 * @Description: Package overview; cron registration, job execution, and Redis singleton locks.
 * @File: tcron.go
 * @Version: 1.0.0
 * @Date: 2026/04/12
 */

// Package tcron schedules background jobs using github.com/robfig/cron/v3 with optional
// jitter, single-run jobs, and distributed mutual exclusion via Redis for singleton execution
// across processes. It integrates OpenTelemetry tracing and Prometheus histogram metrics
// for latency when used with github.com/choveylee/ttrace and github.com/choveylee/tmetric.
//
// The default scheduler starts at package init with the local time zone. Register functions
// return an opaque job identifier that may be passed to RemoveCron to unregister the job.
package tcron

import (
	"context"
	"math/rand"
	"reflect"
	"runtime"
	"runtime/debug"
	"sync"
	"time"

	"github.com/choveylee/tlog"
	"github.com/choveylee/tmetric"
	"github.com/choveylee/ttrace"
	"github.com/choveylee/tutil"
	"github.com/redis/go-redis/v9"
	"github.com/robfig/cron/v3"
	"go.opentelemetry.io/otel/attribute"
	oteltrace "go.opentelemetry.io/otel/trace"
)

var defaultCronManager = &cronManager{
	jobIdCronIdMap: map[string]int{},
}

var _ cron.Job = cronJob{}

func init() {
	defaultCronManager.cronParser = cron.NewParser(
		cron.Second | cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow | cron.Descriptor)

	defaultCronManager.cronClient = cron.New(cron.WithLocation(time.Local))

	defaultCronManager.cronClient.Start()
}

// CronRedisClient defines the Redis operations used for distributed singleton locks.
// Implementations are typically *redis.Client or compatible wrappers.
type CronRedisClient interface {
	SetNX(ctx context.Context, key string, value any, expiration time.Duration) *redis.BoolCmd
	Del(ctx context.Context, keys ...string) *redis.IntCmd
}

// RegisterCron registers a recurring job identified by the caller function name (via reflection).
//
// spec is a cron expression parsed with seconds granularity. delta, if positive, applies
// uniform random jitter in the interval [0, delta) to each computed next run time.
// params are passed to f on each invocation.
func RegisterCron(spec string, f func(ctx context.Context, params ...any) error, delta time.Duration, params ...any) (string, error) {
	return registerCron(spec, f, false, false, delta, nil, 0, params...)
}

// RegisterOnceCron registers a job that runs once at the next schedule tick, then unregisters itself.
func RegisterOnceCron(spec string, f func(ctx context.Context, params ...any) error, params ...any) (string, error) {
	return registerCron(spec, f, false, true, 0, nil, 0, params...)
}

// RegisterSingletonCron registers a recurring job that may execute on at most one instance
// at a time. A Redis SET NX lock keyed by the job function name guards each run; the lock
// is deleted when the run finishes.
//
// lockTtl is the expiration of the SET NX key. It should exceed the worst-case execution
// time of the handler so another instance cannot acquire the lock while work is still in
// progress. If the process crashes, the key expires after lockTtl so another instance may
// take over. If lockTtl is zero or negative, the interval between consecutive schedule
// ticks (Duration) is used; if that interval is also zero, one minute is used.
func RegisterSingletonCron(spec string, f func(ctx context.Context, params ...any) error, redis CronRedisClient, lockTtl time.Duration) (string, error) {
	return registerCron(spec, f, true, false, 0, redis, lockTtl)
}

// RegisterSingletonOnceCron registers a singleton job (see RegisterSingletonCron) that runs
// once and then removes itself from the scheduler.
func RegisterSingletonOnceCron(spec string, f func(ctx context.Context, params ...any) error, redis CronRedisClient, lockTtl time.Duration, params ...any) (string, error) {
	return registerCron(spec, f, true, true, 0, redis, lockTtl, params...)
}

// RemoveCron removes the scheduled job identified by id. It returns false if id is unknown.
func RemoveCron(id string) bool {
	return removeCronId(id)
}

func registerCron(spec string, f func(ctx context.Context, params ...any) error, singleton bool, once bool, delta time.Duration, redis CronRedisClient, lockTtl time.Duration, params ...any) (string, error) {
	name := runtime.FuncForPC(reflect.ValueOf(f).Pointer()).Name()

	schedule, err := defaultCronManager.cronParser.Parse(spec)
	if err != nil {
		tlog.W(context.Background()).Err(err).Msgf("register cron (%s) err (parse %v).", spec, err)

		return "", err
	}

	next := schedule.Next(time.Now())
	duration := schedule.Next(next).Sub(next)

	job := cronJob{
		Id:        tutil.NewOidHex(),
		Name:      name,
		Spec:      spec,
		Func:      f,
		Singleton: singleton,
		Once:      once,
		Duration:  duration,
		Delta:     delta,
		LockTtl:   lockTtl,
		redis:     redis,
		args:      params,
	}

	return processJob(&job, &schedule)
}

func processJob(job *cronJob, schedule *cron.Schedule) (string, error) {
	var entryId cron.EntryID

	if job.Delta > 0 {
		entryId = defaultCronManager.cronClient.Schedule(&deltaSchedule{
			parent: *schedule,
			delta:  int64(job.Delta),
		}, job)
	} else {
		entryId = defaultCronManager.cronClient.Schedule(*schedule, job)
	}

	addCronId(job.Id, int(entryId))

	return job.Id, nil
}

type cronManager struct {
	cronClient *cron.Cron
	cronParser cron.Parser

	jobIdCronIdMap map[string]int

	sync.RWMutex
}

func addCronId(jobId string, cronId int) {
	defaultCronManager.Lock()
	defer defaultCronManager.Unlock()

	defaultCronManager.jobIdCronIdMap[jobId] = cronId
}

func removeCronId(jobId string) bool {
	defaultCronManager.Lock()
	defer defaultCronManager.Unlock()

	entryId, ok := defaultCronManager.jobIdCronIdMap[jobId]
	if ok == false {
		tlog.W(context.Background()).Msgf("remove cron id (%s) err (job id not exist).", jobId)

		return false
	}

	delete(defaultCronManager.jobIdCronIdMap, jobId)

	defaultCronManager.cronClient.Remove(cron.EntryID(entryId))

	return true
}

// deltaSchedule applies jitter: the next activation time is the parent's Next plus a uniform
// random offset in [0, delta) nanoseconds when delta is positive.
type deltaSchedule struct {
	parent cron.Schedule
	delta  int64
}

func (s *deltaSchedule) Next(now time.Time) time.Time {
	if s.delta == 0 {
		return s.parent.Next(now)
	}

	return s.parent.Next(now).Add(time.Duration(rand.Int63n(s.delta)))
}

type cronJob struct {
	Id        string
	Name      string
	Spec      string
	Func      func(ctx context.Context, params ...any) error
	Singleton bool
	Once      bool
	// Duration is the interval between two consecutive schedule ticks used for lock TTL fallback.
	Duration time.Duration
	// Delta is the upper bound of random jitter when positive.
	Delta time.Duration
	// LockTtl is the Redis lock expiration for singleton jobs; see RegisterSingletonCron.
	LockTtl time.Duration
	redis   CronRedisClient
	args    []any
}

func (job cronJob) Run() {
	defer func() {
		if r := recover(); r != nil {
			tlog.E(context.Background()).Detailf("stack: %s", string(debug.Stack())).Msg("recover")
		}
	}()

	curTime := time.Now()

	opts := []oteltrace.SpanStartOption{
		oteltrace.WithSpanKind(oteltrace.SpanKindInternal),
		oteltrace.WithTimestamp(curTime),
		oteltrace.WithAttributes(
			attribute.String("cron.name", job.Name),
			attribute.String("cron.spec", job.Spec),
			attribute.Bool("cron.singleton", job.Singleton),
			attribute.Bool("cron.once", job.Once),
		),
	}

	ctx, span := ttrace.Start(context.Background(), "cron."+job.Name, opts...)
	defer span.End()

	if job.Singleton {
		if job.redis != nil {
			lockTtl := job.LockTtl

			if lockTtl <= 0 {
				lockTtl = job.Duration
			}

			if lockTtl <= 0 {
				lockTtl = time.Minute
			}

			val, err := job.redis.SetNX(ctx, job.Name, curTime.Format(time.RFC3339), lockTtl).Result()
			if err != nil {
				tlog.W(ctx).Err(err).Msg("redis setnx failed while doing job")

				return
			} else if !val {
				return
			}

			defer job.redis.Del(ctx, job.Name)
		} else {
			return
		}
	}

	status := "success"

	err := job.Func(ctx, job.args...)
	if err != nil {
		status = "failed"
		tlog.E(ctx).Err(err).Detailf("job id: %s, job name: %s, job spec: %s, job,duration: %d second, job latency: %f millisecond",
			job.Id, job.Name, job.Spec, job.Duration/time.Second, tmetric.SinceMS(curTime)).Msg("cron job done with error")
	} else {
		tlog.I(ctx).Detailf("job id: %s, job name: %s, job spec: %s, job,duration: %d second, job latency: %f millisecond",
			job.Id, job.Name, job.Spec, job.Duration/time.Second, tmetric.SinceMS(curTime)).Msg("cron job done")
	}

	if job.Once {
		removeCronId(job.Id)
	}

	cronJobHistogram.Observe(tmetric.SinceMS(curTime), job.Name, status)
}

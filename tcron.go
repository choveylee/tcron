/**
 * @Author: lidonglin
 * @Description: Package overview; cron registration, job execution, and Redis singleton locks.
 * @File: tcron.go
 * @Version: 1.0.0
 * @Date: 2026/04/12
 */

// Package tcron provides registration and execution primitives for background jobs
// scheduled by github.com/robfig/cron/v3.
//
// The package supports recurring jobs, one-shot jobs, optional schedule jitter, and
// Redis-backed singleton execution across processes. When used with
// github.com/choveylee/ttrace and github.com/choveylee/tmetric, each execution also emits
// an OpenTelemetry span and a latency metric.
//
// The package-level scheduler starts during package initialization and uses time.Local.
// Each registration function returns an opaque identifier that may be passed to RemoveCron
// to unregister the job.
package tcron

import (
	"context"
	"errors"
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

var (
	errNilRedisClient                = errors.New("tcron: singleton jobs require a non-nil Redis client")
	errRedisClientMissingEvalSupport = errors.New("tcron: singleton jobs require a Redis client with Eval support for conditional lock release")
)

const releaseSingletonLockScript = `
if redis.call("get", KEYS[1]) == ARGV[1] then
	return redis.call("del", KEYS[1])
end

return 0
`

func init() {
	defaultCronManager.cronParser = cron.NewParser(
		cron.Second | cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow | cron.Descriptor)

	defaultCronManager.cronClient = cron.New(cron.WithLocation(time.Local))

	defaultCronManager.cronClient.Start()
}

// CronRedisClient defines the Redis operations required for singleton job execution.
//
// Implementations are typically *redis.Client or compatible wrappers. For safe lock
// release, singleton registration also requires the concrete client to implement Eval so
// the package can perform a compare-and-delete unlock.
type CronRedisClient interface {
	SetNX(ctx context.Context, key string, value any, expiration time.Duration) *redis.BoolCmd
	Del(ctx context.Context, keys ...string) *redis.IntCmd
}

type cronRedisEvalClient interface {
	Eval(ctx context.Context, script string, keys []string, args ...any) *redis.Cmd
}

// RegisterCron registers f to run each time spec matches.
//
// The cron expression is parsed with seconds precision. If delta is positive, each
// computed activation time is delayed by a uniformly distributed jitter in [0, delta).
// params are forwarded to f on each execution.
func RegisterCron(spec string, f func(ctx context.Context, params ...any) error, delta time.Duration, params ...any) (string, error) {
	return registerCron(spec, f, false, false, delta, nil, 0, params...)
}

// RegisterOnceCron registers f to run once at the next time matched by spec.
//
// After the first execution attempt, including panic recovery, the job removes itself
// from the scheduler.
func RegisterOnceCron(spec string, f func(ctx context.Context, params ...any) error, params ...any) (string, error) {
	return registerCron(spec, f, false, true, 0, nil, 0, params...)
}

// RegisterSingletonCron registers f as a recurring distributed singleton job.
//
// At most one process may execute the job at a time. Each execution uses Redis SET NX to
// acquire a lock keyed by the resolved job name, and lock release uses a compare-and-delete
// Lua script so one execution cannot release another execution's lock after TTL expiry.
//
// redis must be non-nil and must support Eval. lockTtl is the expiration of the Redis lock.
// It should exceed the worst-case execution time of the handler so another instance cannot
// acquire the lock while work is still in progress. If the process crashes, the key expires
// after lockTtl so another instance may take over. If lockTtl is zero or negative, the
// interval between consecutive schedule ticks is used; if that interval is also zero, one
// minute is used.
func RegisterSingletonCron(spec string, f func(ctx context.Context, params ...any) error, redis CronRedisClient, lockTtl time.Duration) (string, error) {
	return registerCron(spec, f, true, false, 0, redis, lockTtl)
}

// RegisterSingletonOnceCron registers f as a singleton job that runs once.
//
// The singleton semantics are the same as RegisterSingletonCron. After the first execution
// attempt, including panic recovery, the job removes itself from the scheduler.
func RegisterSingletonOnceCron(spec string, f func(ctx context.Context, params ...any) error, redis CronRedisClient, lockTtl time.Duration, params ...any) (string, error) {
	return registerCron(spec, f, true, true, 0, redis, lockTtl, params...)
}

// RemoveCron removes the scheduled job identified by id.
//
// It reports whether a matching job was found and removed.
func RemoveCron(id string) bool {
	return removeCronId(id)
}

func registerCron(spec string, f func(ctx context.Context, params ...any) error, singleton bool, once bool, delta time.Duration, redis CronRedisClient, lockTtl time.Duration, params ...any) (string, error) {
	var redisEval cronRedisEvalClient
	var err error

	if singleton {
		redisEval, err = validateSingletonRedisClient(redis)
		if err != nil {
			return "", err
		}
	}

	name := runtime.FuncForPC(reflect.ValueOf(f).Pointer()).Name()

	schedule, err := defaultCronManager.cronParser.Parse(spec)
	if err != nil {
		tlog.W(context.Background()).Err(err).Msgf("Failed to register the cron job because the schedule expression %q could not be parsed.", spec)

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
		redisEval: redisEval,
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
		tlog.W(context.Background()).Msgf("Cron job removal was skipped because the identifier %q was not found.", jobId)

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
	LockTtl   time.Duration
	redis     CronRedisClient
	redisEval cronRedisEvalClient
	args      []any
}

func (job cronJob) Run() {
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
	status := "success"
	ran := false
	defer func() {
		if r := recover(); r != nil {
			status = "failed"
			tlog.E(ctx).Detailf("panic: %v, stack: %s", r, string(debug.Stack())).Msg("Cron job execution panicked and was recovered.")
		}

		if job.Once && ran {
			removeCronId(job.Id)
		}

		if ran {
			observeCronJobLatency(tmetric.SinceMS(curTime), job.Name, status)
		}

		span.End()
	}()

	if job.Singleton {
		if job.redis == nil || job.redisEval == nil {
			tlog.W(ctx).Err(errRedisClientMissingEvalSupport).Msg("Singleton job execution was skipped because the Redis client configuration is invalid.")

			return
		}

		lockTtl := job.LockTtl

		if lockTtl <= 0 {
			lockTtl = job.Duration
		}

		if lockTtl <= 0 {
			lockTtl = time.Minute
		}

		lockToken := tutil.NewOidHex()

		val, err := job.redis.SetNX(ctx, job.Name, lockToken, lockTtl).Result()
		if err != nil {
			tlog.W(ctx).Err(err).Msg("Singleton job execution could not acquire the Redis lock because SET NX failed.")

			return
		} else if !val {
			return
		}

		defer releaseSingletonLock(ctx, job, lockToken)
	}

	ran = true

	err := job.Func(ctx, job.args...)
	if err != nil {
		status = "failed"
		tlog.E(ctx).Err(err).Detailf("job id: %s, job name: %s, job spec: %s, job,duration: %d second, job latency: %f millisecond",
			job.Id, job.Name, job.Spec, job.Duration/time.Second, tmetric.SinceMS(curTime)).Msg("Cron job execution completed with an error.")
	} else {
		tlog.I(ctx).Detailf("job id: %s, job name: %s, job spec: %s, job,duration: %d second, job latency: %f millisecond",
			job.Id, job.Name, job.Spec, job.Duration/time.Second, tmetric.SinceMS(curTime)).Msg("Cron job execution completed successfully.")
	}
}

func validateSingletonRedisClient(client CronRedisClient) (cronRedisEvalClient, error) {
	if client == nil {
		return nil, errNilRedisClient
	}

	redisEval, ok := client.(cronRedisEvalClient)
	if !ok {
		return nil, errRedisClientMissingEvalSupport
	}

	return redisEval, nil
}

func releaseSingletonLock(ctx context.Context, job cronJob, lockToken string) {
	deleted, err := job.redisEval.Eval(ctx, releaseSingletonLockScript, []string{job.Name}, lockToken).Int64()
	if err != nil {
		tlog.W(ctx).Err(err).Msg("Singleton job execution could not release the Redis lock.")

		return
	}

	if deleted == 0 {
		tlog.W(ctx).Msg("Redis lock release was skipped because ownership changed or the lock had already expired.")
	}
}

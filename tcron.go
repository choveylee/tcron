/**
 * @Author: lidonglin
 * @Description:
 * @File:  tcron.go
 * @Version: 1.0.0
 * @Date: 2022/11/05 10:58
 */

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
	"github.com/go-redis/redis/v8"
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

type CronRedisClient interface {
	SetNX(ctx context.Context, key string, value interface{}, expiration time.Duration) *redis.BoolCmd
	Del(ctx context.Context, keys ...string) *redis.IntCmd
}

// RegisterCron register cron
// delta define execute random bias [0, delta)
func RegisterCron(spec string, f func(ctx context.Context, params ...interface{}) error, delta time.Duration, params ...interface{}) (string, error) {
	return registerCron(spec, f, false, false, delta, nil, params...)
}

// RegisterOnceCron register cron execute once
func RegisterOnceCron(spec string, f func(ctx context.Context, params ...interface{}) error, params ...interface{}) (string, error) {
	return registerCron(spec, f, false, true, 0, nil, params...)
}

// RegisterSingletonCron 注册定时任务, 同个任务同个时间最多被一个实例执行
func RegisterSingletonCron(spec string, f func(ctx context.Context, params ...interface{}) error, redis CronRedisClient) (string, error) {
	return registerCron(spec, f, true, false, 0, redis)
}

// RegisterSingletonOnceCron 注册定时任务, 同个任务同个时间最多被一个实例执行,执行一次后自动移除
func RegisterSingletonOnceCron(spec string, f func(ctx context.Context, params ...interface{}) error, redis CronRedisClient, params ...interface{}) (string, error) {
	return registerCron(spec, f, true, true, 0, redis, params...)
}

// RemoveCron remove cron
func RemoveCron(id string) bool {
	return removeCronId(id)
}

func registerCron(spec string, f func(ctx context.Context, params ...interface{}) error, singleton bool, once bool, delta time.Duration, redis CronRedisClient, params ...interface{}) (string, error) {
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
	Func      func(ctx context.Context, params ...interface{}) error
	Singleton bool
	Once      bool
	Duration  time.Duration
	Delta     time.Duration
	redis     CronRedisClient
	args      []interface{}
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
			val, err := job.redis.SetNX(ctx, job.Name, curTime.Format(time.RFC3339), job.Duration/2).Result()
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

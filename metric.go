/**
 * @Author: lidonglin
 * @Description: Prometheus histogram registration for cron job latency.
 * @File: metric.go
 * @Version: 1.0.0
 * @Date: 2026/04/12
 */

package tcron

import (
	"context"

	"github.com/choveylee/tlog"
	"github.com/choveylee/tmetric"
)

var (
	// cronJobHistogram records per-execution latency in milliseconds, labeled by job name
	// and outcome status ("success" or "failed"). Bucket boundaries follow the defaults
	// provided by tmetric.
	cronJobHistogram, cronJobHistogramErr = initCronJobHistogram()
)

func init() {
	if cronJobHistogramErr != nil {
		tlog.W(context.Background()).Err(cronJobHistogramErr).Msg("Cron job latency metrics could not be registered; metric collection has been disabled.")
	}
}

func initCronJobHistogram() (*tmetric.HistogramVec, error) {
	return tmetric.NewHistogramVec(
		"cron_job_latency",
		"CronJob",
		[]string{
			"name",
			"status",
		},
	)
}

func observeCronJobLatency(latency float64, name string, status string) {
	if cronJobHistogram == nil {
		return
	}

	if err := cronJobHistogram.Observe(latency, name, status); err != nil {
		tlog.W(context.Background()).Err(err).Msg("Cron job latency metrics could not be recorded.")
	}
}

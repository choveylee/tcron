/**
 * @Author: lidonglin
 * @Description: Prometheus histogram registration for cron job latency.
 * @File: metric.go
 * @Version: 1.0.0
 * @Date: 2026/04/12
 */

package tcron

import (
	"github.com/choveylee/tmetric"
)

var (
	// cronJobHistogram records per-run latency in milliseconds, labeled by job name and
	// outcome status ("success" or "failed"). Bucket boundaries follow tmetric defaults.
	cronJobHistogram, _ = tmetric.NewHistogramVec(
		"cron_job_latency",
		"CronJob",
		[]string{
			"name",
			"status",
		},
	)
)

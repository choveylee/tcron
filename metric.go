/**
 * @Author: lidonglin
 * @Description:
 * @File:  metric.go
 * @Version: 1.0.0
 * @Date: 2022/11/05 12:23
 */

package tcron

import (
	"github.com/choveylee/tmetric"
)

var (
	cronJobStatusCounter, _ = tmetric.NewGaugeVec(
		"cron_job_status",
		"CronJob",
		[]string{
			"name",
			"status",
		},
	)

	cronJobHistogram, _ = tmetric.NewHistogramVec(
		"cron_job_latency",
		"CronJob",
		[]string{
			"name",
			"status",
		},
	)
	cronJobInfoGauge, _ = tmetric.NewGaugeVec(
		"cron_job_info",
		"CronJob",
		[]string{
			"name",
			"singleton",
			"once",
		},
	)
)

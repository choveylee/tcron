package tcron

import (
	"context"

	"github.com/choveylee/tlog"
	"github.com/choveylee/tmetric"
)

// cronJobHistogram records latency in milliseconds for admitted job executions.
var cronJobHistogram *tmetric.HistogramVec

func init() {
	histogram, err := tmetric.NewHistogramVec("cron_job_latency", "CronJob", []string{"name", "status"})
	if err != nil {
		tlog.W(context.Background()).Err(err).Msg("Register cron job latency metric error")

		return
	}

	cronJobHistogram = histogram
}

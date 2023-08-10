package pgxjob

import (
	"time"
)

func (w *Worker) SetMinHeartbeatDelayForTest(d time.Duration) {
	w.minHeartbeatDelay = d
}

func (w *Worker) SetHeartbeatDelayJitterForTest(d time.Duration) {
	w.heartbeatDelayJitter = d
}

func (w *Worker) SetWorkerDeadWithoutHeartbeatDurationForTest(d time.Duration) {
	w.workerDeadWithoutHeartbeatDuration = d
}

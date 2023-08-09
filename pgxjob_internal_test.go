package pgxjob

import (
	"testing"
	"time"
)

func SetMinWorkerHeartbeatDelayForTest(t testing.TB, d time.Duration) {
	previous := minWorkerHeartbeatDelay
	t.Cleanup(func() {
		minWorkerHeartbeatDelay = previous
	})
	minWorkerHeartbeatDelay = d
}

func SetWorkerHeartbeatDelayJitterForTest(t testing.TB, d time.Duration) {
	previous := workerHeartbeatDelayJitter
	t.Cleanup(func() {
		workerHeartbeatDelayJitter = previous
	})
	workerHeartbeatDelayJitter = d
}

func SetWorkerDeadWithoutHeartbeatDurationForTest(t testing.TB, d time.Duration) {
	previous := workerDeadWithoutHeartbeatDuration
	t.Cleanup(func() {
		workerDeadWithoutHeartbeatDuration = previous
	})
	workerDeadWithoutHeartbeatDuration = d
}

package pgxjob

import (
	"time"
)

func (c *WorkerConfig) SetMinHeartbeatDelayForTest(d time.Duration) {
	c.minHeartbeatDelay = d
}

func (c *WorkerConfig) SetHeartbeatDelayJitterForTest(d time.Duration) {
	c.heartbeatDelayJitter = d
}

func (c *WorkerConfig) SetWorkerDeadWithoutHeartbeatDurationForTest(d time.Duration) {
	c.workerDeadWithoutHeartbeatDuration = d
}

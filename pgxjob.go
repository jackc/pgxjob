// Package pgxjob provides a job queue implementation using PostgreSQL.
package pgxjob

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

const pgChannelName = "pgxjob_jobs"

// Scheduler is used to schedule jobs and start workers.
type Scheduler struct {
	jobTypeByName map[string]*JobType
}

// NewScheduler returns a new Scheduler.
func NewScheduler() *Scheduler {
	return &Scheduler{
		jobTypeByName: make(map[string]*JobType),
	}
}

// JobType is a type of job.
type JobType struct {
	// Name is the name of the job type. It must be set and unique.
	Name string

	// DefaultQueue is the default queue to use when enqueuing jobs of this type. If not set "default" is used.
	DefaultQueue string

	// DefaultPriority is the default priority to use when enqueuing jobs of this type. The minimum priority is 1. If not
	// set 100 is used.
	DefaultPriority int32

	// RunJob is the function that will be called when a job of this type is run. It must be set.
	RunJob func(ctx context.Context, paramsJSON []byte) error
}

// RegisterJobType registers a job type. It must be called before any jobs are scheduled or workers are started.
func (m *Scheduler) RegisterJobType(jobType JobType) error {
	if jobType.Name == "" {
		return fmt.Errorf("jobType.Name must be set")
	}

	if jobType.DefaultQueue == "" {
		jobType.DefaultQueue = "default"
	}

	if jobType.DefaultPriority < 1 {
		jobType.DefaultPriority = 100
	}

	if jobType.RunJob == nil {
		return fmt.Errorf("jobType.RunJob must be set")
	}

	if _, ok := m.jobTypeByName[jobType.Name]; ok {
		return fmt.Errorf("job type with name %s already registered", jobType.Name)
	}
	m.jobTypeByName[jobType.Name] = &jobType

	return nil
}

// MustRegisterJobType calls RegisterJobType and panics if it returns an error.
func (m *Scheduler) MustRegisterJobType(jobType JobType) {
	err := m.RegisterJobType(jobType)
	if err != nil {
		panic(err)
	}
}

func RunJobFunc[T any](fn func(ctx context.Context, params *T) error) func(ctx context.Context, paramsJSON []byte) error {
	return func(ctx context.Context, paramsJSON []byte) error {
		var params T
		err := json.Unmarshal(paramsJSON, &params)
		if err != nil {
			return fmt.Errorf("unmarshal params failed: %w", err)
		}

		return fn(ctx, &params)
	}
}

// DB is the type pgxjob uses to interact with the database when it does not specifically need a *pgx.Conn.
type DB interface {
	Begin(ctx context.Context) (pgx.Tx, error)
	CopyFrom(ctx context.Context, tableName pgx.Identifier, columnNames []string, rowSrc pgx.CopyFromSource) (int64, error)
	Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error)
	Query(ctx context.Context, sql string, optionsAndArgs ...interface{}) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
	SendBatch(ctx context.Context, b *pgx.Batch) (br pgx.BatchResults)
}

// JobSchedule is a schedule for a job.
type JobSchedule struct {
	// QueueName is the name of the queue to use when enqueuing the job. If not set the job type's default queue is used.
	QueueName string

	// Priority is the priority to use when enqueuing the job. The minimum priority is 1. If not set the job type's
	// default priority is used.
	Priority int32

	// RunAt is the time to run the job. If not set the job is scheduled to run immediately.
	RunAt time.Time
}

// ScheduleNow schedules a job to be run immediately.
func (m *Scheduler) ScheduleNow(ctx context.Context, db DB, jobTypeName string, jobParams any) error {
	return m.Schedule(ctx, db, jobTypeName, jobParams, JobSchedule{})
}

// Schedule schedules a job to be run according to schedule.
func (m *Scheduler) Schedule(ctx context.Context, db DB, jobTypeName string, jobParams any, schedule JobSchedule) error {
	jobType, ok := m.jobTypeByName[jobTypeName]
	if !ok {
		return fmt.Errorf("pgxjob: job type with name %s not registered", jobTypeName)
	}

	var queueName string
	if schedule.QueueName != "" {
		queueName = schedule.QueueName
	} else {
		queueName = jobType.DefaultQueue
	}

	var priority int32
	if schedule.Priority > 0 {
		priority = schedule.Priority
	} else {
		priority = jobType.DefaultPriority
	}

	now := time.Now()
	var runAt time.Time
	if !schedule.RunAt.IsZero() {
		runAt = schedule.RunAt
	} else {
		runAt = now
	}

	batch := &pgx.Batch{}
	batch.Queue(
		`insert into pgxjob_jobs (queue_name, priority, type, params, queued_at, run_at) values ($1, $2, $3, $4, $5, $6)`,
		queueName, priority, jobTypeName, jobParams, now, runAt,
	)
	batch.Queue(`select pg_notify($1, null)`, pgChannelName)
	err := db.SendBatch(ctx, batch).Close()
	if err != nil {
		return fmt.Errorf("pgxjob: failed to schedule job: %w", err)
	}

	return nil
}

func (m *Scheduler) NewWorker(ctx context.Context, jobParams any) error {
	return nil
}

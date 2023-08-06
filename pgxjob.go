// Package pgxjob provides a job queue implementation using PostgreSQL.
package pgxjob

import (
	"context"
	"encoding/json"
	"fmt"
	"runtime/debug"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgxutil"
)

const pgChannelName = "pgxjob_jobs"

var lockDuration = 1 * time.Minute

// Scheduler is used to schedule jobs and start workers.
type Scheduler struct {
	queues        map[string]struct{}
	jobTypeByName map[string]*JobType
}

// NewScheduler returns a new Scheduler.
func NewScheduler() *Scheduler {
	return &Scheduler{
		queues:        map[string]struct{}{"default": {}},
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
	RunJob func(ctx context.Context, job *Job) error
}

// RegisterQueue registers a queue. It must be called before any jobs are scheduled or workers are started.
func (m *Scheduler) RegisterQueue(queueName string) error {
	if queueName == "" {
		return fmt.Errorf("queueName must be set")
	}

	if _, ok := m.queues[queueName]; ok {
		return fmt.Errorf("queue with name %s already registered", queueName)
	}

	m.queues[queueName] = struct{}{}

	return nil
}

// MustRegisterQueue calls RegisterQueue and panics if it returns an error.
func (m *Scheduler) MustRegisterQueue(queueName string) {
	err := m.RegisterQueue(queueName)
	if err != nil {
		panic(err)
	}
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

// RunJobFunc is a convenience function for creating a JobType.RunJob function that parses the params into a T.
func RunJobFunc[T any](fn func(ctx context.Context, job *Job, params *T) error) func(ctx context.Context, job *Job) error {
	return func(ctx context.Context, job *Job) error {
		var params T
		err := json.Unmarshal(job.Params, &params)
		if err != nil {
			return fmt.Errorf("unmarshal job params failed: %w", err)
		}

		return fn(ctx, job, &params)
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

	if _, ok := m.queues[queueName]; !ok {
		return fmt.Errorf("pgxjob: queue with name %s not registered", queueName)
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
		`insert into pgxjob_jobs (queue_name, priority, type, params, queued_at, run_at, locked_until) values ($1, $2, $3, $4, $5, $6, $7)`,
		queueName, priority, jobTypeName, jobParams, now, runAt, runAt,
	)
	batch.Queue(`select pg_notify($1, null)`, pgChannelName)
	err := db.SendBatch(ctx, batch).Close()
	if err != nil {
		return fmt.Errorf("pgxjob: failed to schedule job: %w", err)
	}

	return nil
}

type GetConnFunc func(ctx context.Context) (conn *pgxpool.Conn, release func(), err error)

func GetConnFromPoolFunc(pool *pgxpool.Pool) GetConnFunc {
	return func(ctx context.Context) (conn *pgxpool.Conn, release func(), err error) {
		conn, err = pool.Acquire(ctx)
		if err != nil {
			return nil, nil, fmt.Errorf("pgxjob: failed to acquire connection: %w", err)
		}

		return conn, func() { conn.Release() }, nil
	}
}

type WorkerConfig struct {
	// GetConnFunc is a function that returns a *pgx.Conn and a release function. It must be set.
	GetConnFunc GetConnFunc

	// QueueNames is a list of queues to work. If empty all queues are worked.
	QueueNames []string

	// MaxConcurrentJobs is the maximum number of jobs of all types to work concurrently. If not set 1 is used.
	MaxConcurrentJobs int

	// PollInterval is the interval between polling for new jobs. If not set 10 seconds is used.
	PollInterval time.Duration

	// HandleWorkerError is a function that is called when the worker encounters an error there is nothing that can be
	// done within the worker. For example, a network outage may cause the worker to be unable to fetch a job or record
	// the outcome of an execution. The Worker.Run execution should not be stopped because of this. Instead, it should try
	// again later when the network may have been restored. These types of errors are passed to HandleWorkerError.
	HandleWorkerError func(worker *Worker, err error)
}

type Worker struct {
	WorkerConfig
	scheduler  *Scheduler
	signalChan chan struct{}

	cancelCtx context.Context
	cancel    context.CancelFunc

	mux         sync.Mutex
	runningJobs map[int64]*Job
	running     bool

	jobWaitGroup sync.WaitGroup
}

func (m *Scheduler) NewWorker(config WorkerConfig) (*Worker, error) {
	for _, queueName := range config.QueueNames {
		_, ok := m.queues[queueName]
		if !ok {
			return nil, fmt.Errorf("pgxjob: queue with name %s not registered", queueName)
		}
	}
	if len(config.QueueNames) == 0 {
		for queueName := range m.queues {
			config.QueueNames = append(config.QueueNames, queueName)
		}
	}

	if config.MaxConcurrentJobs == 0 {
		config.MaxConcurrentJobs = 1
	}

	if config.PollInterval == 0 {
		config.PollInterval = 10 * time.Second
	}

	w := &Worker{
		WorkerConfig: config,
		scheduler:    m,
		signalChan:   make(chan struct{}, 1),
		runningJobs:  make(map[int64]*Job, config.MaxConcurrentJobs),
	}
	w.cancelCtx, w.cancel = context.WithCancel(context.Background())

	return w, nil
}

// Run starts the worker. It runs until ctx is canceled. One worker cannot Run multiple times concurrently.
func (w *Worker) Run() error {
	w.mux.Lock()
	if w.running {
		w.mux.Unlock()
		return fmt.Errorf("pgxjob: worker already running")
	}
	w.mux.Unlock()

	for {
		// Check if the context is done before processing any jobs.
		select {
		case <-w.cancelCtx.Done():
			return nil
		default:
		}

		w.fetchAndStartJobs()

		select {
		case <-w.cancelCtx.Done():
			return nil
		case <-time.NewTimer(w.PollInterval).C:
		case <-w.signalChan:
		}
	}

}

// Shutdown stops the worker. It waits for all jobs to finish before returning. Cancel ctx to force shutdown without
// waiting for jobs to finish.
func (w *Worker) Shutdown(ctx context.Context) error {
	w.cancel()

	doneChan := make(chan struct{})
	go func() {
		w.jobWaitGroup.Wait()
		close(doneChan)
	}()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-doneChan:
		return nil
	}
}

type Job struct {
	ID          int64
	QueueName   string
	Priority    int32
	Type        *JobType
	Params      []byte
	QueuedAt    time.Time
	RunAt       time.Time
	LockedUntil time.Time
	ErrorCount  int32
	LastError   pgtype.Text
}

// fetchAndLockJobsSQL is used to fetch and lock jobs in a single query.
//
// Exactly how concurrency and locking work with CTEs can be confusing, but the "for update skip locked" is held for the
// entire statement (actually the lock is held for the entire transaction) per Tom Lane
// (https://www.postgresql.org/message-id/1604.1499787945%40sss.pgh.pa.us).
const fetchAndLockJobsSQL = `with lock_jobs as (
	select id
	from pgxjob_jobs
	where locked_until < now()
		and queue_name = any($1)
	order by priority desc, run_at
	limit $2
	for update skip locked
)
update pgxjob_jobs
set locked_until = now() + $3
where id in (select id from lock_jobs)
returning id, queue_name, priority, type, params, queued_at, run_at, locked_until, error_count, last_error`

func (w *Worker) fetchAndStartJobs() error {
	w.mux.Lock()
	defer w.mux.Unlock()
	availableJobs := w.MaxConcurrentJobs - len(w.runningJobs)

	if availableJobs == 0 {
		return nil
	}

	conn, release, err := w.GetConnFunc(w.cancelCtx)
	if err != nil {
		return fmt.Errorf("failed to get connection: %w", err)
	}
	defer release()

	jobs, err := pgxutil.Select(w.cancelCtx, conn,
		fetchAndLockJobsSQL,
		[]any{w.QueueNames, availableJobs, lockDuration},
		pgx.RowToAddrOfStructByPos[Job],
	)
	if err != nil {
		return fmt.Errorf("failed to lock jobs: %w", err)
	}

	for _, job := range jobs {
		w.startJob(job)
	}

	return nil
}

// startJob runs a job in a goroutine. w.mux must be locked.
func (w *Worker) startJob(job *Job) {
	w.jobWaitGroup.Add(1)
	w.runningJobs[job.ID] = job

	go func(job *Job) {
		defer w.jobWaitGroup.Done()

		var err error
		func() {
			defer func() {
				if r := recover(); r != nil {
					stack := debug.Stack()
					err = fmt.Errorf("panic: %v\n%s", r, string(stack))
				}
			}()
			err = job.Type.RunJob(w.cancelCtx, job)
		}()

		if err == nil {
			w.recordJobSuccess(job)
		} else {
			w.recordJobError(job, err)
		}

	}(job)
}

// recordJobError records an error that occurred while processing a job. It does not take a context because it should
// still execute even if the context that controls the overall Worker.Run is cancelled.
func (w *Worker) recordJobError(job *Job, jobErr error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	conn, release, err := w.GetConnFunc(ctx)
	if err != nil {
		w.handleWorkerError(fmt.Errorf("pgxjob: recording job %d failure: failed to get connection: %w", job.ID, err))
	}
	defer release()

	_, err = pgxutil.ExecRow(ctx, conn,
		`update pgxjob_job set error_count = error_count + 1, last_error = $1, locked_until = 'Infinity' where id = $2`,
		jobErr.Error(), job.ID,
	)
	if err != nil {
		w.handleWorkerError(fmt.Errorf("pgxjob: recording job %d failure: %w", job.ID, err))
	}
}

// recordJobSuccess records that a job was successfully processed.
func (w *Worker) recordJobSuccess(job *Job) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	conn, release, err := w.GetConnFunc(ctx)
	if err != nil {
		w.handleWorkerError(fmt.Errorf("pgxjob: recording job %d success: failed to get connection: %w", job.ID, err))
	}
	defer release()

	_, err = pgxutil.ExecRow(ctx, conn, `delete from pgxjob_jobs where id = $1`, job.ID)
	if err != nil {
		w.handleWorkerError(fmt.Errorf("pgxjob: recording job %d success: %w", job.ID, err))
	}
}

func (w *Worker) handleWorkerError(err error) {
	if w.HandleWorkerError != nil {
		w.HandleWorkerError(w, err)
	}
}

// Signal causes the worker to wake up and process requests. It is safe to call this from multiple goroutines.
func (w *Worker) Signal() error {
	select {
	case w.signalChan <- struct{}{}:
	default:
	}

	return nil
}

// Package pgxjob provides a job queue implementation using PostgreSQL.
package pgxjob

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"runtime/debug"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgxutil"
)

const pgChannelName = "pgxjob_jobs"

var lockDuration = 1 * time.Minute

// Scheduler is used to schedule jobs and start workers.
type Scheduler struct {
	getConnFunc GetConnFunc

	jobQueuesByName map[string]*JobQueue
	jobQueuesByID   map[int32]*JobQueue

	jobTypesByName map[string]*JobType
	jobTypesByID   map[int32]*JobType
}

// NewScheduler returns a new Scheduler.
func NewScheduler(ctx context.Context, getConnFunc GetConnFunc) (*Scheduler, error) {
	s := &Scheduler{
		getConnFunc:     getConnFunc,
		jobQueuesByName: map[string]*JobQueue{},
		jobQueuesByID:   map[int32]*JobQueue{},
		jobTypesByName:  make(map[string]*JobType),
		jobTypesByID:    make(map[int32]*JobType),
	}

	err := s.RegisterJobQueue(ctx, "default")
	if err != nil {
		return nil, err
	}

	return s, nil
}

// JobType is a type of job.
type JobType struct {
	// ID is the ID of the job type. It is set automatically.
	ID int32

	// Name is the name of the job type.
	Name string

	// DefaultQueue is the default queue to use when enqueuing jobs of this type.
	DefaultQueue *JobQueue

	// DefaultPriority is the default priority to use when enqueuing jobs of this type.
	DefaultPriority int16

	// RunJob is the function that will be called when a job of this type is run.
	RunJob func(ctx context.Context, job *Job) error
}

type JobQueue struct {
	ID   int32
	Name string
}

// RegisterJobQueue registers a queue. It must be called before any jobs are scheduled or workers are started.
func (s *Scheduler) RegisterJobQueue(ctx context.Context, name string) error {
	if name == "" {
		return fmt.Errorf("pgxjob: name must be set")
	}

	if _, ok := s.jobQueuesByName[name]; ok {
		return fmt.Errorf("pgxjob: queue with name %s already registered", name)
	}

	conn, release, err := s.getConnFunc(ctx)
	if err != nil {
		return fmt.Errorf("failed to get connection: %w", err)
	}
	defer release()

	var jobQueueID int32
	selectIDErr := conn.QueryRow(ctx, `select id from pgxjob_queues where name = $1`, name).Scan(&jobQueueID)
	if errors.Is(selectIDErr, pgx.ErrNoRows) {
		_, insertErr := conn.Exec(ctx, `insert into pgxjob_queues (name) values ($1) on conflict do nothing`, name)
		if insertErr != nil {
			return fmt.Errorf("pgxjob: failed to insert queue %s: %w", name, insertErr)
		}

		selectIDErr = conn.QueryRow(ctx, `select id from pgxjob_queues where name = $1`, name).Scan(&jobQueueID)
	}
	if selectIDErr != nil {
		return fmt.Errorf("pgxjob: failed to select id for queue %s: %w", name, selectIDErr)
	}

	jq := &JobQueue{
		ID:   jobQueueID,
		Name: name,
	}
	s.jobQueuesByName[jq.Name] = jq
	s.jobQueuesByID[jq.ID] = jq
	return nil
}

type RegisterJobTypeParams struct {
	// Name is the name of the job type. It must be set and unique.
	Name string

	// DefaultQueueName is the name of the default queue to use when enqueuing jobs of this type. If not set "default" is
	// used.
	DefaultQueueName string

	// DefaultPriority is the default priority to use when enqueuing jobs of this type. The minimum priority is 1. If not
	// set 100 is used.
	DefaultPriority int16

	// RunJob is the function that will be called when a job of this type is run. It must be set.
	RunJob RunJobFunc
}

// RegisterJobType registers a job type. It must be called before any jobs are scheduled or workers are started.
func (s *Scheduler) RegisterJobType(ctx context.Context, params RegisterJobTypeParams) error {
	if params.Name == "" {
		return fmt.Errorf("params.Name must be set")
	}

	if params.DefaultQueueName == "" {
		params.DefaultQueueName = "default"
	}

	defaultQueue, ok := s.jobQueuesByName[params.DefaultQueueName]
	if !ok {
		return fmt.Errorf("pgxjob: queue with name %s not registered", params.DefaultQueueName)
	}

	if params.DefaultPriority < 1 {
		params.DefaultPriority = 100
	}

	if params.RunJob == nil {
		return fmt.Errorf("params.RunJob must be set")
	}

	if _, ok := s.jobTypesByName[params.Name]; ok {
		return fmt.Errorf("job type with name %s already registered", params.Name)
	}

	conn, release, err := s.getConnFunc(ctx)
	if err != nil {
		return fmt.Errorf("failed to get connection: %w", err)
	}
	defer release()

	var jobTypeID int32
	selectIDErr := conn.QueryRow(ctx, `select id from pgxjob_types where name = $1`, params.Name).Scan(&jobTypeID)
	if errors.Is(selectIDErr, pgx.ErrNoRows) {
		_, insertErr := conn.Exec(ctx, `insert into pgxjob_types (name) values ($1) on conflict do nothing`, params.Name)
		if insertErr != nil {
			return fmt.Errorf("pgxjob: failed to insert type %s: %w", params.Name, insertErr)
		}

		selectIDErr = conn.QueryRow(ctx, `select id from pgxjob_types where name = $1`, params.Name).Scan(&jobTypeID)
	}
	if selectIDErr != nil {
		return fmt.Errorf("pgxjob: failed to select id for queue %s: %w", params.Name, selectIDErr)
	}

	jt := &JobType{
		ID:              jobTypeID,
		Name:            params.Name,
		DefaultQueue:    defaultQueue,
		DefaultPriority: params.DefaultPriority,
		RunJob:          params.RunJob,
	}

	s.jobTypesByName[jt.Name] = jt
	s.jobTypesByID[jt.ID] = jt

	return nil
}

type RunJobFunc func(ctx context.Context, job *Job) error

// UnmarshalParams returns a JobType.RunJob function that unmarshals job.Params into a T and calls fn.
func UnmarshalParams[T any](fn func(ctx context.Context, job *Job, params *T) error) RunJobFunc {
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
	Priority int16

	// RunAt is the time to run the job. If not set the job is scheduled to run immediately.
	RunAt time.Time
}

// ScheduleNow schedules a job to be run immediately.
func (m *Scheduler) ScheduleNow(ctx context.Context, db DB, jobTypeName string, jobParams any) error {
	return m.Schedule(ctx, db, jobTypeName, jobParams, JobSchedule{})
}

// Schedule schedules a job to be run according to schedule.
func (m *Scheduler) Schedule(ctx context.Context, db DB, jobTypeName string, jobParams any, schedule JobSchedule) error {
	jobType, ok := m.jobTypesByName[jobTypeName]
	if !ok {
		return fmt.Errorf("pgxjob: job type with name %s not registered", jobTypeName)
	}

	var jobQueue *JobQueue
	if schedule.QueueName == "" {
		jobQueue = jobType.DefaultQueue
	} else {
		var ok bool
		jobQueue, ok = m.jobQueuesByName[schedule.QueueName]
		if !ok {
			return fmt.Errorf("pgxjob: queue with name %s not registered", schedule.QueueName)
		}
	}

	var priority int16
	if schedule.Priority > 0 {
		priority = schedule.Priority
	} else {
		priority = jobType.DefaultPriority
	}

	now := time.Now()

	batch := &pgx.Batch{}
	if schedule.RunAt.IsZero() {
		batch.Queue(
			`insert into pgxjob_jobs (queue_id, priority, type_id, params, queued_at) values ($1, $2, $3, $4, $5)`,
			jobQueue.ID, priority, jobType.ID, jobParams, now,
		)
	} else {
		batch.Queue(
			`insert into pgxjob_jobs (queue_id, priority, type_id, params, queued_at, next_run_at) values ($1, $2, $3, $4, $5, $6)`,
			jobQueue.ID, priority, jobType.ID, jobParams, now, schedule.RunAt,
		)
	}

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
	queueIDs []int32

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
	var queueIDs []int32
	if len(config.QueueNames) == 0 {
		queueIDs = make([]int32, 0, len(m.jobQueuesByID))
		for _, jq := range m.jobQueuesByName {
			queueIDs = append(queueIDs, jq.ID)
		}
	} else {
		queueIDs = make([]int32, 0, len(config.QueueNames))
		for _, queueName := range config.QueueNames {
			jq, ok := m.jobQueuesByName[queueName]
			if !ok {
				return nil, fmt.Errorf("pgxjob: queue with name %s not registered", queueName)
			}
			queueIDs = append(queueIDs, jq.ID)
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
		queueIDs:     queueIDs,
		scheduler:    m,
		signalChan:   make(chan struct{}, 1),
		runningJobs:  make(map[int64]*Job, config.MaxConcurrentJobs),
	}
	w.cancelCtx, w.cancel = context.WithCancel(context.Background())

	return w, nil
}

// Start starts the worker. It runs until Shutdown is called. One worker cannot Start multiple times concurrently.
func (w *Worker) Start() error {
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

		err := w.fetchAndStartJobs()
		if err != nil {
			w.handleWorkerError(fmt.Errorf("pgxjob: failed to fetch and start jobs: %w", err))
		}

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
	ID         int64
	Queue      *JobQueue
	Type       *JobType
	Params     []byte
	QueuedAt   time.Time
	RunAt      time.Time
	LastError  string
	ErrorCount int32
	Priority   int16
}

// fetchAndLockJobsSQL is used to fetch and lock jobs in a single query. It takes 3 bound parameters. $1 is an array of
// of queue ids. $2 is the maximum number of jobs to fetch. $3 is the lock duration.
//
// Exactly how concurrency and locking work with CTEs can be confusing, but the "for update skip locked" is held for the
// entire statement (actually the lock is held for the entire transaction) per Tom Lane
// (https://www.postgresql.org/message-id/1604.1499787945%40sss.pgh.pa.us).
const fetchAndLockJobsSQL = `with lock_jobs as (
	select id
	from pgxjob_jobs
	where (next_run_at < now() or next_run_at is null)
		and queue_id = any($1)
	order by priority desc, coalesce(next_run_at, queued_at)
	limit $2
	for update skip locked
)
update pgxjob_jobs
set run_at = coalesce(run_at, queued_at),
	next_run_at = now() + $3
where id in (select id from lock_jobs)
returning id, queue_id, type_id, params, queued_at, run_at, coalesce(last_error, ''), coalesce(error_count, 0), priority`

func (w *Worker) fetchAndStartJobs() error {
	w.mux.Lock()
	defer w.mux.Unlock()
	availableJobs := w.MaxConcurrentJobs - len(w.runningJobs)

	if availableJobs == 0 {
		return nil
	}

	conn, release, err := w.scheduler.getConnFunc(w.cancelCtx)
	if err != nil {
		return fmt.Errorf("failed to get connection: %w", err)
	}
	defer release()

	jobs, err := pgxutil.Select(w.cancelCtx, conn,
		fetchAndLockJobsSQL,
		[]any{w.queueIDs, availableJobs, lockDuration},
		func(row pgx.CollectableRow) (*Job, error) {
			var job Job
			var jobQueueID int32
			var jobTypeID int32
			err := row.Scan(
				&job.ID, &jobQueueID, &jobTypeID, &job.Params, &job.QueuedAt, &job.RunAt, &job.LastError, &job.ErrorCount, &job.Priority,
			)
			if err != nil {
				return nil, err
			}

			job.Queue = w.scheduler.jobQueuesByID[jobQueueID]
			if jobType, ok := w.scheduler.jobTypesByID[jobTypeID]; ok {
				job.Type = jobType
			} else {
				// This should never happen because job types are created and never deleted. But if somehow it does happen then
				// create a fake JobType with a RunJob that returns an error.
				job.Type = &JobType{
					ID: jobTypeID,
					RunJob: func(ctx context.Context, job *Job) error {
						return fmt.Errorf("pgxjob: job type with id %d not registered", jobTypeID)
					},
				}
			}

			return &job, nil
		},
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

		startedAt := time.Now()
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
		finishedAt := time.Now()
		w.recordJobResults(job, startedAt, finishedAt, err)

	}(job)
}

// recordJobResults records the results of running a job. It does not take a context because it should still execute
// even if the context that controls the overall Worker.Run is cancelled.
func (w *Worker) recordJobResults(job *Job, startedAt, finishedAt time.Time, jobErr error) {
	defer func() {
		w.mux.Lock()
		delete(w.runningJobs, job.ID)
		w.mux.Unlock()
		w.Signal()
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	conn, release, err := w.scheduler.getConnFunc(ctx)
	if err != nil {
		w.handleWorkerError(fmt.Errorf("pgxjob: recording job %d results: failed to get connection: %w", job.ID, err))
	}
	defer release()

	if jobErr == nil {
		_, err = pgxutil.ExecRow(ctx, conn, `delete from pgxjob_jobs where id = $1`, job.ID)
		if err != nil {
			w.handleWorkerError(fmt.Errorf("pgxjob: recording job %d results: %w", job.ID, err))
		}

		_, err = pgxutil.ExecRow(ctx, conn,
			`insert into pgxjob_job_runs (job_id, queued_at, run_at, started_at, finished_at, run_number, queue_id, type_id, priority, params)
values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)`,
			job.ID, job.QueuedAt, job.RunAt, startedAt, finishedAt, job.ErrorCount+1, job.Queue.ID, job.Type.ID, job.Priority, job.Params)
		if err != nil {
			w.handleWorkerError(fmt.Errorf("pgxjob: recording job %d results: %w", job.ID, err))
		}
	} else {
		_, err = pgxutil.ExecRow(ctx, conn,
			`insert into pgxjob_job_runs (job_id, queued_at, run_at, started_at, finished_at, run_number, queue_id, type_id, priority, params, error)
values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)`,
			job.ID, job.QueuedAt, job.RunAt, startedAt, finishedAt, job.ErrorCount+1, job.Queue.ID, job.Type.ID, job.Priority, job.Params, jobErr.Error())
		if err != nil {
			w.handleWorkerError(fmt.Errorf("pgxjob: recording job %d results: %w", job.ID, err))
		}

		var errorWithRetry *ErrorWithRetry
		if errors.As(jobErr, &errorWithRetry) {
			_, err = pgxutil.ExecRow(ctx, conn,
				`update pgxjob_jobs set error_count = $1, last_error = $2, next_run_at = $3 where id = $4`,
				job.ErrorCount+1, errorWithRetry.Err.Error(), errorWithRetry.RetryAt, job.ID,
			)
			if err != nil {
				w.handleWorkerError(fmt.Errorf("pgxjob: recording job %d results: %w", job.ID, err))
			}
		} else {
			_, err = pgxutil.ExecRow(ctx, conn, `delete from pgxjob_jobs where id = $1`, job.ID)
			if err != nil {
				w.handleWorkerError(fmt.Errorf("pgxjob: recording job %d results: %w", job.ID, err))
			}
		}
	}
}

func (w *Worker) handleWorkerError(err error) {
	if w.HandleWorkerError != nil {
		w.HandleWorkerError(w, err)
	}
}

// Signal causes the worker to wake up and process requests. It is safe to call this from multiple goroutines.
func (w *Worker) Signal() {
	select {
	case w.signalChan <- struct{}{}:
	default:
	}
}

type ErrorWithRetry struct {
	Err     error
	RetryAt time.Time
}

func (e *ErrorWithRetry) Error() string {
	return e.Err.Error()
}

func (e *ErrorWithRetry) Unwrap() error {
	return e.Err
}

// FilterError returns a RunJobFunc that calls runJob. If runJob returns an error then it calls filterError and returns
// its error. filterError is typically used to determine if the error should be retried or not.
func FilterError(runJob RunJobFunc, filterError func(job *Job, jobErr error) error) RunJobFunc {
	return func(ctx context.Context, job *Job) error {
		jobErr := runJob(ctx, job)
		if jobErr != nil {
			return filterError(job, jobErr)
		}

		return nil
	}
}

// RetryLinearBackoff returns a RunJobFunc that calls runJob. If runJob returns an error then maxRetries and baseDelay
// are used to when and whether to retry the job. If the error is already an ErrorWithRetry then it is returned
// unmodified.
//
// e.g. If maxRetries is 3 and baseDelay is 1 minutes then the job will be retried after 1 minute, then after an
// additional 2 minutes, and finally after an additional 3 minutes. That is, the last retry will take occur 6 minutes
// after the first failure.
func RetryLinearBackoff(runJob RunJobFunc, maxRetries int32, baseDelay time.Duration) RunJobFunc {
	return FilterError(runJob, func(job *Job, jobErr error) error {
		if job.ErrorCount >= maxRetries {
			return jobErr
		}

		var errorWithRetry *ErrorWithRetry
		if errors.As(jobErr, &errorWithRetry) {
			return jobErr
		}

		return &ErrorWithRetry{
			Err:     jobErr,
			RetryAt: time.Now().Add(time.Duration(job.ErrorCount+1) * baseDelay),
		}
	})
}

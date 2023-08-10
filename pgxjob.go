// Package pgxjob provides a job runner using PostgreSQL.
package pgxjob

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"runtime/debug"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype/zeronull"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgxutil"
)

const PGNotifyChannel = "pgxjob_job_available"
const defaultGroupName = "default"

// Scheduler is used to schedule jobs and start workers.
type Scheduler struct {
	getConnFunc GetConnFunc

	jobGroupsByName map[string]*JobGroup
	jobGroupsByID   map[int32]*JobGroup

	jobTypesByName map[string]*JobType
	jobTypesByID   map[int32]*JobType
}

// NewScheduler returns a new Scheduler.
func NewScheduler(ctx context.Context, getConnFunc GetConnFunc) (*Scheduler, error) {
	s := &Scheduler{
		getConnFunc:     getConnFunc,
		jobGroupsByName: map[string]*JobGroup{},
		jobGroupsByID:   map[int32]*JobGroup{},
		jobTypesByName:  make(map[string]*JobType),
		jobTypesByID:    make(map[int32]*JobType),
	}

	err := s.RegisterJobGroup(ctx, defaultGroupName)
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

	// DefaultGroup is the default group to use when enqueuing jobs of this type.
	DefaultGroup *JobGroup

	// RunJob is the function that will be called when a job of this type is run.
	RunJob func(ctx context.Context, job *Job) error
}

type JobGroup struct {
	ID   int32
	Name string
}

// RegisterJobGroup registers a group. It must be called before any jobs are scheduled or workers are started.
func (s *Scheduler) RegisterJobGroup(ctx context.Context, name string) error {
	if name == "" {
		return fmt.Errorf("pgxjob: name must be set")
	}

	if _, ok := s.jobGroupsByName[name]; ok {
		return fmt.Errorf("pgxjob: group with name %s already registered", name)
	}

	conn, release, err := s.getConnFunc(ctx)
	if err != nil {
		return fmt.Errorf("failed to get connection: %w", err)
	}
	defer release()

	var jobGroupID int32
	selectIDErr := conn.QueryRow(ctx, `select id from pgxjob_groups where name = $1`, name).Scan(&jobGroupID)
	if errors.Is(selectIDErr, pgx.ErrNoRows) {
		_, insertErr := conn.Exec(ctx, `insert into pgxjob_groups (name) values ($1) on conflict do nothing`, name)
		if insertErr != nil {
			return fmt.Errorf("pgxjob: failed to insert group %s: %w", name, insertErr)
		}

		selectIDErr = conn.QueryRow(ctx, `select id from pgxjob_groups where name = $1`, name).Scan(&jobGroupID)
	}
	if selectIDErr != nil {
		return fmt.Errorf("pgxjob: failed to select id for group %s: %w", name, selectIDErr)
	}

	jq := &JobGroup{
		ID:   jobGroupID,
		Name: name,
	}
	s.jobGroupsByName[jq.Name] = jq
	s.jobGroupsByID[jq.ID] = jq
	return nil
}

type RegisterJobTypeParams struct {
	// Name is the name of the job type. It must be set and unique.
	Name string

	// DefaultGroupName is the name of the default group to use when enqueuing jobs of this type. If not set "default" is
	// used.
	DefaultGroupName string

	// RunJob is the function that will be called when a job of this type is run. It must be set.
	RunJob RunJobFunc
}

// RegisterJobType registers a job type. It must be called before any jobs are scheduled or workers are started.
func (s *Scheduler) RegisterJobType(ctx context.Context, params RegisterJobTypeParams) error {
	if params.Name == "" {
		return fmt.Errorf("params.Name must be set")
	}

	if params.DefaultGroupName == "" {
		params.DefaultGroupName = defaultGroupName
	}

	defaultGroup, ok := s.jobGroupsByName[params.DefaultGroupName]
	if !ok {
		return fmt.Errorf("pgxjob: group with name %s not registered", params.DefaultGroupName)
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
		return fmt.Errorf("pgxjob: failed to select id for type %s: %w", params.Name, selectIDErr)
	}

	jt := &JobType{
		ID:           jobTypeID,
		Name:         params.Name,
		DefaultGroup: defaultGroup,
		RunJob:       params.RunJob,
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
	// GroupName is the name of the group to use when enqueuing the job. If not set the job type's default group is used.
	GroupName string

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

	var jobGroup *JobGroup
	if schedule.GroupName == "" {
		jobGroup = jobType.DefaultGroup
	} else {
		var ok bool
		jobGroup, ok = m.jobGroupsByName[schedule.GroupName]
		if !ok {
			return fmt.Errorf("pgxjob: group with name %s not registered", schedule.GroupName)
		}
	}

	if schedule.RunAt.IsZero() {
		batch := &pgx.Batch{}
		batch.Queue(
			`insert into pgxjob_asap_jobs (group_id, type_id, params) values ($1, $2, $3)`,
			jobGroup.ID, jobType.ID, jobParams,
		)
		batch.Queue(`select pg_notify($1, null)`, PGNotifyChannel)
		err := db.SendBatch(ctx, batch).Close()
		if err != nil {
			return fmt.Errorf("pgxjob: failed to schedule asap job: %w", err)
		}
	} else {
		_, err := db.Exec(ctx,
			`insert into pgxjob_run_at_jobs (group_id, type_id, params, run_at, next_run_at, error_count) values ($1, $2, $3, $4, $4, 0)`,
			jobGroup.ID, jobType.ID, jobParams, schedule.RunAt,
		)
		if err != nil {
			return fmt.Errorf("pgxjob: failed to schedule run at job: %w", err)
		}
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
	// GroupName is the group to work. If empty, "default" is used.
	GroupName string

	// MaxConcurrentJobs is the maximum number of jobs to work concurrently. If not set 10 is used.
	MaxConcurrentJobs int

	// MaxPrefetchedJobs is the maximum number of prefetched jobs (i.e. jobs that are fetched from the database and
	// locked, but not yet being worked). If not set 1000 is used.
	MaxPrefetchedJobs int

	// PollInterval is the interval between polling for new jobs. If not set 10 seconds is used.
	PollInterval time.Duration

	// MaxBufferedJobResults is the maximum number of job results that can be buffered before the job results must be
	// flushed to the database. If not set 100 is used.
	MaxBufferedJobResults int

	// MaxBufferedJobResultAge is the maximum age of a buffered job result before the job results must be flushed to the
	// database. If not set 1 second is used.
	MaxBufferedJobResultAge time.Duration

	// HandleWorkerError is a function that is called when the worker encounters an error there is nothing that can be
	// done within the worker. For example, a network outage may cause the worker to be unable to fetch a job or record
	// the outcome of an execution. The Worker.Run execution should not be stopped because of this. Instead, it should try
	// again later when the network may have been restored. These types of errors are passed to HandleWorkerError.
	HandleWorkerError func(worker *Worker, err error)

	minHeartbeatDelay                  time.Duration
	heartbeatDelayJitter               time.Duration
	workerDeadWithoutHeartbeatDuration time.Duration
}

type Worker struct {
	ID int32

	WorkerConfig
	group *JobGroup

	scheduler  *Scheduler
	signalChan chan struct{}

	cancelCtx context.Context
	cancel    context.CancelFunc

	mux                     sync.Mutex
	jobRunnerGoroutineCount int
	started                 bool

	jobRunnerGoroutineWaitGroup sync.WaitGroup
	jobChan                     chan *Job

	jobResultsChan          chan *jobResult
	writeJobResultsDoneChan chan struct{}

	heartbeatDoneChan         chan struct{}
	fetchAndStartJobsDoneChan chan struct{}
}

func (m *Scheduler) NewWorker(ctx context.Context, config WorkerConfig) (*Worker, error) {
	if config.GroupName == "" {
		config.GroupName = defaultGroupName
	}
	jg, ok := m.jobGroupsByName[config.GroupName]
	if !ok {
		return nil, fmt.Errorf("pgxjob: group with name %s not registered", config.GroupName)
	}
	group := jg

	if config.MaxConcurrentJobs == 0 {
		config.MaxConcurrentJobs = 10
	}

	if config.MaxPrefetchedJobs == 0 {
		config.MaxPrefetchedJobs = 1000
	}

	if config.PollInterval == 0 {
		config.PollInterval = 10 * time.Second
	}

	if config.MaxBufferedJobResults == 0 {
		config.MaxBufferedJobResults = 100
	}

	if config.MaxBufferedJobResultAge == 0 {
		config.MaxBufferedJobResultAge = 1 * time.Second
	}

	config.minHeartbeatDelay = 45 * time.Second
	config.heartbeatDelayJitter = 30 * time.Second
	config.workerDeadWithoutHeartbeatDuration = 5 * (config.minHeartbeatDelay + config.heartbeatDelayJitter)

	conn, release, err := m.getConnFunc(ctx)
	if err != nil {
		return nil, fmt.Errorf("pgxjob: failed to get connection: %w", err)
	}
	defer release()
	workerID, err := pgxutil.SelectRow(ctx, conn,
		`insert into pgxjob_workers (heartbeat) values (now()) returning id`,
		nil,
		pgx.RowTo[int32],
	)
	if err != nil {
		return nil, fmt.Errorf("pgxjob: failed to insert into pgxjob_workers: %w", err)
	}

	w := &Worker{
		ID:                        workerID,
		WorkerConfig:              config,
		group:                     group,
		scheduler:                 m,
		signalChan:                make(chan struct{}, 1),
		jobChan:                   make(chan *Job),
		jobResultsChan:            make(chan *jobResult),
		heartbeatDoneChan:         make(chan struct{}),
		fetchAndStartJobsDoneChan: make(chan struct{}),
	}
	w.cancelCtx, w.cancel = context.WithCancel(context.Background())

	return w, nil
}

// Start starts the worker. It runs until Shutdown is called. One worker cannot Start multiple times concurrently.
func (w *Worker) Start() error {
	w.mux.Lock()
	if w.started {
		w.mux.Unlock()
		return fmt.Errorf("pgxjob: worker cannot be started more than once")
	}
	w.started = true
	w.mux.Unlock()

	go w.heartbeat()

	w.writeJobResultsDoneChan = make(chan struct{})
	go w.writeJobResults()

	return w.fetchAndStartJobs()
}

func (w *Worker) heartbeat() {
	defer close(w.heartbeatDoneChan)

	for {
		select {
		case <-w.cancelCtx.Done():
			return
		case <-time.After(w.minHeartbeatDelay + time.Duration(rand.Int63n(int64(w.heartbeatDelayJitter)))):
			func() {
				defer func() {
					if r := recover(); r != nil {
						w.handleWorkerError(fmt.Errorf("pgxjob: panic in heartbeat for %d: %v\n%s", w.ID, r, debug.Stack()))
					}
				}()

				err := func() error {
					ctx, cancel := context.WithTimeout(w.cancelCtx, 15*time.Second)
					defer cancel()

					conn, release, err := w.scheduler.getConnFunc(ctx)
					if err != nil {
						return fmt.Errorf("pgxjob: heartbeat for %d: failed to get connection: %w", w.ID, err)
					}
					defer release()

					_, err = conn.Exec(ctx, `update pgxjob_workers set heartbeat = now() where id = $1`, w.ID)
					if err != nil {
						return fmt.Errorf("pgxjob: heartbeat for %d: failed to update: %w", w.ID, err)
					}

					_, err = conn.Exec(ctx, `with t as (
	delete from pgxjob_workers where heartbeat + $1 < now() returning id
), u1 as (
	update pgxjob_asap_jobs
	set worker_id = null
	from t
	where worker_id = t.id
)
update pgxjob_run_at_jobs
set worker_id = null
from t
where worker_id = t.id
`, w.workerDeadWithoutHeartbeatDuration)
					if err != nil {
						return fmt.Errorf("pgxjob: heartbeat for %d: failed to cleanup dead workers: %w", w.ID, err)
					}

					return nil
				}()
				if err != nil {
					w.handleWorkerError(err)
				}
			}()
		}
	}
}

func (w *Worker) writeJobResults() {
	defer close(w.writeJobResultsDoneChan)

	type pgxjobJobRun struct {
		JobID      int64
		InsertedAt time.Time
		RunAt      time.Time
		StartedAt  time.Time
		FinishedAt time.Time
		RunNumber  int32
		GroupID    int32
		TypeID     int32
		Params     []byte
		Error      zeronull.Text
	}

	type pgxjobJobUpdate struct {
		ID        int64
		LastError string
		NextRunAt time.Time
		ASAP      bool
	}

	jobResults := make([]*jobResult, 0, w.MaxBufferedJobResults)
	asapJobIDsToDelete := make([]int64, 0, w.MaxBufferedJobResults)
	runAtJobIDsToDelete := make([]int64, 0, w.MaxBufferedJobResults)
	pgxjobJobRunsToInsert := make([]pgxjobJobRun, 0, w.MaxBufferedJobResults)

	flushTimer := time.NewTimer(time.Hour)
	flushTimer.Stop()

	flush := func() {
		if !flushTimer.Stop() {
			select {
			case <-flushTimer.C:
			default:
			}
		}

		// Always clear the results even if there is an error. In case of error there is nothing that can be done.
		defer func() {
			clear(jobResults)
			jobResults = jobResults[:0]
			clear(asapJobIDsToDelete)
			asapJobIDsToDelete = asapJobIDsToDelete[:0]
			clear(runAtJobIDsToDelete)
			runAtJobIDsToDelete = runAtJobIDsToDelete[:0]
			clear(pgxjobJobRunsToInsert)
			pgxjobJobRunsToInsert = pgxjobJobRunsToInsert[:0]
		}()

		// Job errors should be rare. So do not use persistent slice like jobIDsToDelete and pgxjobJobRunsToInsert.
		var pgxjobJobUpdates []pgxjobJobUpdate
		for _, jobResult := range jobResults {
			job := jobResult.job
			var errForInsert zeronull.Text
			if jobResult.err == nil {
				if job.ASAP {
					asapJobIDsToDelete = append(asapJobIDsToDelete, jobResult.job.ID)
				} else {
					runAtJobIDsToDelete = append(runAtJobIDsToDelete, jobResult.job.ID)
				}
			} else {
				errForInsert = zeronull.Text(jobResult.err.Error())
				var errorWithRetry *ErrorWithRetry
				if errors.As(jobResult.err, &errorWithRetry) {
					pgxjobJobUpdates = append(pgxjobJobUpdates, pgxjobJobUpdate{
						ID:        job.ID,
						LastError: errorWithRetry.Err.Error(),
						NextRunAt: errorWithRetry.RetryAt,
						ASAP:      job.ASAP,
					})
				} else {
					if job.ASAP {
						asapJobIDsToDelete = append(asapJobIDsToDelete, jobResult.job.ID)
					} else {
						runAtJobIDsToDelete = append(runAtJobIDsToDelete, jobResult.job.ID)
					}
				}
			}
			pgxjobJobRunsToInsert = append(pgxjobJobRunsToInsert, pgxjobJobRun{
				JobID:      job.ID,
				InsertedAt: job.InsertedAt,
				RunAt:      job.RunAt,
				StartedAt:  jobResult.startTime,
				FinishedAt: jobResult.finishedAt,
				RunNumber:  job.ErrorCount + 1,
				GroupID:    job.Group.ID,
				TypeID:     job.Type.ID,
				Params:     job.Params,
				Error:      errForInsert,
			})
		}

		batch := &pgx.Batch{}
		for _, jobUpdate := range pgxjobJobUpdates {
			if jobUpdate.ASAP {
				batch.Queue(
					`with t as (
	delete from pgxjob_asap_jobs where id = $1 returning *
)
insert into pgxjob_run_at_jobs (id, inserted_at, run_at, next_run_at, group_id, type_id, error_count, last_error, params)
select id, inserted_at, inserted_at, $2, group_id, type_id, 1, $3, params
from t`,
					jobUpdate.ID, jobUpdate.NextRunAt, jobUpdate.LastError,
				)
			} else {
				batch.Queue(
					`update pgxjob_run_at_jobs set error_count = error_count + 1, last_error = $1, next_run_at = $2 where id = $3`,
					jobUpdate.LastError, jobUpdate.NextRunAt, jobUpdate.ID,
				)
			}
		}
		if len(asapJobIDsToDelete) > 0 {
			batch.Queue(`delete from pgxjob_asap_jobs where id = any($1)`, asapJobIDsToDelete)
		}
		if len(runAtJobIDsToDelete) > 0 {
			batch.Queue(`delete from pgxjob_run_at_jobs where id = any($1)`, runAtJobIDsToDelete)
		}

		// COPY FROM is faster than INSERT for multiple rows. But it has the overhead of an extra round trip and
		// (auto-commit) transaction. So for small numbers of rows it is faster to bundle INSERTs with the batch that is
		// already being used.
		const jobsRunCopyThreshhold = 5

		if len(pgxjobJobRunsToInsert) < jobsRunCopyThreshhold {
			for _, jobRun := range pgxjobJobRunsToInsert {
				batch.Queue(
					`insert into pgxjob_job_runs (job_id, job_inserted_at, run_at, started_at, finished_at, run_number, group_id, type_id, params, error) values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)`,
					jobRun.JobID, jobRun.InsertedAt, jobRun.RunAt, jobRun.StartedAt, jobRun.FinishedAt, jobRun.RunNumber, jobRun.GroupID, jobRun.TypeID, jobRun.Params, jobRun.Error,
				)
			}
		}

		// The entirety of getting a connection and performing the updates should be very quick. But set a timeout as a
		// failsafe.
		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Minute)
		defer cancel()

		conn, release, err := w.scheduler.getConnFunc(ctx)
		if err != nil {
			w.handleWorkerError(fmt.Errorf("pgxjob: recording job results: failed to get connection: %w", err))
			return
		}
		defer release()

		// Note: purposely not using an explicit transaction. The batch and the copy are each transactional. The only value
		// of the explicit transaction would be to *not* save the batch changes if the copy failed. It is preferable to
		// preserve those changes even if the copy fails. It also saves a round trip for the begin and the commit.

		err = conn.SendBatch(ctx, batch).Close()
		if err != nil {
			w.handleWorkerError(fmt.Errorf("pgxjob: recording job results: failed to send batch: %w", err))
			return
		}

		if len(pgxjobJobRunsToInsert) >= jobsRunCopyThreshhold {
			_, err = conn.CopyFrom(
				ctx,
				pgx.Identifier{"pgxjob_job_runs"},
				[]string{"job_id", "job_inserted_at", "run_at", "started_at", "finished_at", "run_number", "group_id", "type_id", "params", "error"},
				pgx.CopyFromSlice(len(pgxjobJobRunsToInsert), func(i int) ([]any, error) {
					row := &pgxjobJobRunsToInsert[i]
					return []any{row.JobID, row.InsertedAt, row.RunAt, row.StartedAt, row.FinishedAt, row.RunNumber, row.GroupID, row.TypeID, row.Params, row.Error}, nil
				}),
			)
			if err != nil {
				w.handleWorkerError(fmt.Errorf("pgxjob: recording job results: failed to copy pgxjob_job_runs: %w", err))
				return
			}
		}
	}

	defer flush()

	appendJobResult := func(jobResult *jobResult) {
		jobResults = append(jobResults, jobResult)
		if len(jobResults) >= w.MaxBufferedJobResults {
			flush()
		} else if len(jobResults) == 1 {
			flushTimer.Reset(w.MaxBufferedJobResultAge)
		}
	}

loop1:
	for {
		select {
		case <-w.cancelCtx.Done():
			break loop1
		case jobResult := <-w.jobResultsChan:
			appendJobResult(jobResult)
		case <-flushTimer.C:
			flush()
		}
	}

	doneChan := make(chan struct{})
	go func() {
		// must be done before waiting for w.jobRunnerGoroutineWaitGroup because fetchAndStartJobs can start a job worker
		// which calls w.jobRunnerGoroutineWaitGroup.Add. From the docs for Add: "Note that calls with a positive delta that
		// occur when the counter is zero must happen before a Wait." Violating this rule can cause a race condtion.
		<-w.fetchAndStartJobsDoneChan

		w.jobRunnerGoroutineWaitGroup.Wait()
		close(doneChan)
	}()

loop2:
	for {
		select {
		case <-doneChan:
			break loop2
		case jobResult := <-w.jobResultsChan:
			appendJobResult(jobResult)
		case <-flushTimer.C:
			flush()
		}
	}
}

// Shutdown stops the worker. It waits for all jobs to finish before returning. Cancel ctx to force shutdown without
// waiting for jobs to finish or worker to cleanup.
func (w *Worker) Shutdown(ctx context.Context) error {
	w.cancel()

	// Wait for all worker goroutines to finish.
	doneChan := make(chan struct{})
	go func() {
		// must be done before waiting for w.jobRunnerGoroutineWaitGroup because fetchAndStartJobs can start a job worker
		// which calls w.jobRunnerGoroutineWaitGroup.Add. From the docs for Add: "Note that calls with a positive delta that
		// occur when the counter is zero must happen before a Wait." Violating this rule can cause a race condtion.
		<-w.fetchAndStartJobsDoneChan

		w.jobRunnerGoroutineWaitGroup.Wait()
		<-w.writeJobResultsDoneChan
		<-w.heartbeatDoneChan
		close(doneChan)
	}()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-doneChan:
	}

	// Cleanup can't be started until all worker goroutines have finished. Otherwise, the cleanup may unlock a job that is
	// still being worked on this worker. Another worker could pick up the job and it could be executed multiple times.
	cleanupErrChan := make(chan error)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Minute)
		defer cancel()

		conn, release, err := w.scheduler.getConnFunc(ctx)
		if err != nil {
			cleanupErrChan <- fmt.Errorf("pgxjob: shutdown failed to get connection for cleanup: %w", err)
			return
		}
		defer release()

		batch := &pgx.Batch{}
		batch.Queue(`delete from pgxjob_workers where id = $1`, w.ID)
		batch.Queue(`update pgxjob_asap_jobs set worker_id = null where worker_id = $1`, w.ID)
		batch.Queue(`update pgxjob_run_at_jobs set worker_id = null where worker_id = $1`, w.ID)
		err = conn.SendBatch(ctx, batch).Close()
		if err != nil {
			cleanupErrChan <- fmt.Errorf("pgxjob: shutdown failed to cleanup worker and unlock jobs: %w", err)
			return
		}

		close(cleanupErrChan)
	}()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case err := <-cleanupErrChan:
		return err
	}
}

type Job struct {
	ID         int64
	Group      *JobGroup
	Type       *JobType
	Params     []byte
	InsertedAt time.Time
	RunAt      time.Time
	LastError  string
	ErrorCount int32
	ASAP       bool
}

type jobResult struct {
	job        *Job
	startTime  time.Time
	finishedAt time.Time
	err        error
}

func (w *Worker) fetchAndStartJobs() error {
	defer close(w.fetchAndStartJobsDoneChan)

	for {
		// Check if the context is done before processing any jobs.
		select {
		case <-w.cancelCtx.Done():
			return nil
		default:
		}

		jobs, err := w.fetchJobs()
		if err != nil {
			// context.Canceled means that w.cancelCtx was cancelled. This happens when shutting down.
			if errors.Is(err, context.Canceled) {
				return nil
			}
			w.handleWorkerError(fmt.Errorf("pgxjob: failed to fetch jobs: %w", err))
		}
		noJobsAvailableInDatabase := len(jobs) < w.MaxPrefetchedJobs

		for _, job := range jobs {
			select {
			case w.jobChan <- job:
			case <-w.cancelCtx.Done():
				return nil
			default:
				w.mux.Lock()
				if w.jobRunnerGoroutineCount < w.MaxConcurrentJobs {
					w.startJobRunner()
				}
				w.mux.Unlock()
				select {
				case w.jobChan <- job:
				case <-w.cancelCtx.Done():
					return nil
				}
			}
		}

		if noJobsAvailableInDatabase {
			select {
			case <-w.cancelCtx.Done():
				return nil
			case <-time.NewTimer(w.PollInterval).C:
			case <-w.signalChan:
			}
		}
	}
}

// fetchAndLockASAPJobsSQL is used to fetch and lock pgxjob_asap_jobs in a single query. It takes 3 bound parameters. $1
// is group id. $2 is the maximum number of jobs to fetch. $3 is worker_id that is locking the job.
//
// Exactly how concurrency and locking work with CTEs can be confusing, but the "for update skip locked" is held for the
// entire statement (actually the lock is held for the entire transaction) per Tom Lane
// (https://www.postgresql.org/message-id/1604.1499787945%40sss.pgh.pa.us).
const fetchAndLockASAPJobsSQL = `with lock_jobs as (
	select id
	from pgxjob_asap_jobs
	where group_id = $1
		and worker_id is null
	limit $2
	for update skip locked
)
update pgxjob_asap_jobs
set worker_id = $3
where id in (select id from lock_jobs)
returning id, type_id, params, inserted_at`

// fetchAndLockRunAtJobsSQL is used to fetch and lock jobs in a single query. It takes 3 bound parameters. $1 is group id. $2
// is the maximum number of jobs to fetch. $3 is worker_id that is locking the job.
//
// Exactly how concurrency and locking work with CTEs can be confusing, but the "for update skip locked" is held for the
// entire statement (actually the lock is held for the entire transaction) per Tom Lane
// (https://www.postgresql.org/message-id/1604.1499787945%40sss.pgh.pa.us).
const fetchAndLockRunAtJobsSQL = `with lock_jobs as (
	select id
	from pgxjob_run_at_jobs
	where next_run_at < now()
		and group_id = $1
		and worker_id is null
	limit $2
	for update skip locked
)
update pgxjob_run_at_jobs
set worker_id = $3
where id in (select id from lock_jobs)
returning id, type_id, params, inserted_at, run_at, coalesce(last_error, ''), error_count`

func (w *Worker) fetchJobs() ([]*Job, error) {
	conn, release, err := w.scheduler.getConnFunc(w.cancelCtx)
	if err != nil {
		return nil, fmt.Errorf("failed to get connection: %w", err)
	}
	defer release()

	jobTypeFromID := func(jobTypeID int32) *JobType {
		if jobType, ok := w.scheduler.jobTypesByID[jobTypeID]; ok {
			return jobType
		} else {
			// This should never happen because job types are created and never deleted. But if somehow it does happen then
			// create a fake JobType with a RunJob that returns an error.
			return &JobType{
				ID: jobTypeID,
				RunJob: func(ctx context.Context, job *Job) error {
					return fmt.Errorf("pgxjob: job type with id %d not registered", jobTypeID)
				},
			}
		}
	}

	jobs, err := pgxutil.Select(w.cancelCtx, conn,
		fetchAndLockASAPJobsSQL,
		[]any{w.group.ID, w.MaxPrefetchedJobs, w.ID},
		func(row pgx.CollectableRow) (*Job, error) {
			var job Job
			var jobTypeID int32
			err := row.Scan(
				&job.ID, &jobTypeID, &job.Params, &job.InsertedAt,
			)
			if err != nil {
				return nil, err
			}
			job.RunAt = job.InsertedAt
			job.ASAP = true

			job.Group = w.group
			job.Type = jobTypeFromID(jobTypeID)

			return &job, nil
		},
	)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch and lock pgxjob_asap_jobs: %w", err)
	}

	if len(jobs) < w.MaxPrefetchedJobs {
		runAtJobs, err := pgxutil.Select(w.cancelCtx, conn,
			fetchAndLockRunAtJobsSQL,
			[]any{w.group.ID, w.MaxPrefetchedJobs - len(jobs), w.ID},
			func(row pgx.CollectableRow) (*Job, error) {
				var job Job
				var jobTypeID int32
				err := row.Scan(
					&job.ID, &jobTypeID, &job.Params, &job.InsertedAt, &job.RunAt, &job.LastError, &job.ErrorCount,
				)
				if err != nil {
					return nil, err
				}
				job.ASAP = false

				job.Group = w.group
				job.Type = jobTypeFromID(jobTypeID)

				return &job, nil
			},
		)
		if err != nil {
			// If some jobs were successfully locked. Return those without an error.
			if len(jobs) > 0 {
				return jobs, nil
			}
			return nil, fmt.Errorf("failed to fetch and lock pgxjob_run_at_jobs: %w", err)
		}
		jobs = append(jobs, runAtJobs...)
	}

	return jobs, nil
}

// startJobRunner starts a new job runner. w.mux must be locked before calling.
func (w *Worker) startJobRunner() {
	w.jobRunnerGoroutineWaitGroup.Add(1)
	w.jobRunnerGoroutineCount++

	go func() {
		defer func() {
			w.mux.Lock()
			w.jobRunnerGoroutineCount--
			w.mux.Unlock()
			w.jobRunnerGoroutineWaitGroup.Done()
		}()

		for {
			select {
			case job := <-w.jobChan:
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
				w.jobResultsChan <- &jobResult{job, startedAt, finishedAt, err}

			case <-w.cancelCtx.Done():
				return

			case <-time.NewTimer(5 * time.Second).C:
				return
			}
		}
	}()
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

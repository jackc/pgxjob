// Package pgxjob provides a job runner using PostgreSQL.
package pgxjob

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"runtime/debug"
	"slices"
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

// DefaultContextScheduler is the default scheduler. It is returned by Ctx when no scheduler is set in the context. It
// must be set before use.
var DefaultContextScheduler *Scheduler

type ctxKey struct{}

// Scheduler is used to schedule jobs and start workers.
type Scheduler struct {
	acquireConn AcquireConnFunc

	jobGroupsByName map[string]*JobGroup
	jobGroupsByID   map[int32]*JobGroup

	jobTypesByName map[string]*JobType
	jobTypesByID   map[int32]*JobType

	handleError func(err error)

	setupDoneChan chan struct{}

	config *SchedulerConfig
}

type SchedulerConfig struct {
	// AcquireConn is used to get a connection to the database. It must be set.
	AcquireConn AcquireConnFunc

	// JobGroups is a lists of job groups that can be used by the scheduler. The job group "default" is always available.
	JobGroups []string

	// JobTypes is a list of job types that can be used by the scheduler. It must be set.
	JobTypes []*JobTypeConfig

	// HandleError is a function that is called when an error occurs that cannot be handled or returned. For example, a
	// network outage may cause a worker to be unable to fetch a job or record the outcome of an execution. The worker
	// should not be stopped because of this. Instead, it should try again later when the network may have been restored.
	// These types of errors are passed to HandleError. If not set errors are logged to stderr.
	HandleError func(err error)
}

// NewScheduler returns a new Scheduler.
func NewScheduler(config *SchedulerConfig) (*Scheduler, error) {
	if len(config.JobTypes) == 0 {
		return nil, fmt.Errorf("pgxjob: at least one job type must be registered")
	}

	if !slices.Contains(config.JobGroups, defaultGroupName) {
		config.JobGroups = append(config.JobGroups, defaultGroupName)
	}

	for _, jobGroupName := range config.JobGroups {
		if jobGroupName == "" {
			return nil, fmt.Errorf("pgxjob: job group name cannot be empty")
		}
	}

	for _, jobType := range config.JobTypes {
		if jobType.Name == "" {
			return nil, fmt.Errorf("pgxjob: job type name must be set")
		}
		if jobType.DefaultGroupName == "" {
			jobType.DefaultGroupName = defaultGroupName
		}

		if !slices.Contains(config.JobGroups, jobType.DefaultGroupName) {
			return nil, fmt.Errorf("pgxjob: job type has default group name %s that is not in job groups", jobType.DefaultGroupName)
		}

		if jobType.RunJob == nil {
			return nil, fmt.Errorf("params.RunJob must be set")
		}
	}

	s := &Scheduler{
		acquireConn:     config.AcquireConn,
		jobGroupsByName: map[string]*JobGroup{},
		jobGroupsByID:   map[int32]*JobGroup{},
		jobTypesByName:  make(map[string]*JobType),
		jobTypesByID:    make(map[int32]*JobType),
		setupDoneChan:   make(chan struct{}),
		config:          config,
	}

	if config.HandleError == nil {
		s.handleError = func(err error) {
			fmt.Fprintf(os.Stderr, "pgxjob: HandleError: %v\n", err)
		}
	} else {
		s.handleError = config.HandleError
	}

	go func() {
		for {
			err := s.setup()
			if err == nil {
				return
			}
			s.handleError(fmt.Errorf("pgxjob: scheduler setup failed: %w", err))
			time.Sleep(5 * time.Second)
		}
	}()

	return s, nil
}

// setup makes one attempt to setup the scheduler.
func (s *Scheduler) setup() error {
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Minute)
	defer cancel()

	conn, release, err := s.acquireConn(ctx)
	if err != nil {
		return fmt.Errorf("failed to get connection: %w", err)
	}
	defer release()

	for _, groupName := range s.config.JobGroups {
		err := s.registerJobGroup(ctx, conn, groupName)
		if err != nil {
			return err
		}
	}

	for _, jobType := range s.config.JobTypes {
		err := s.registerJobType(ctx, conn, jobType)
		if err != nil {
			return err
		}
	}

	close(s.setupDoneChan)
	return nil
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

// registerJobGroup registers a group. It must be called before any jobs are scheduled or workers are started.
func (s *Scheduler) registerJobGroup(ctx context.Context, conn DB, name string) error {
	var jobGroupID int32
	selectIDErr := conn.QueryRow(ctx, `select id from pgxjob_groups where name = $1`, name).Scan(&jobGroupID)
	if errors.Is(selectIDErr, pgx.ErrNoRows) {
		_, insertErr := conn.Exec(ctx, `insert into pgxjob_groups (name) values ($1) on conflict do nothing`, name)
		if insertErr != nil {
			return fmt.Errorf("failed to insert group %s: %w", name, insertErr)
		}

		selectIDErr = conn.QueryRow(ctx, `select id from pgxjob_groups where name = $1`, name).Scan(&jobGroupID)
	}
	if selectIDErr != nil {
		return fmt.Errorf("failed to select id for group %s: %w", name, selectIDErr)
	}

	jq := &JobGroup{
		ID:   jobGroupID,
		Name: name,
	}
	s.jobGroupsByName[jq.Name] = jq
	s.jobGroupsByID[jq.ID] = jq
	return nil
}

type JobTypeConfig struct {
	// Name is the name of the job type. It must be set and unique.
	Name string

	// DefaultGroupName is the name of the default group to use when enqueuing jobs of this type. If not set "default" is
	// used.
	DefaultGroupName string

	// RunJob is the function that will be called when a job of this type is run. It must be set.
	RunJob RunJobFunc
}

// registerJobType registers a job type.
func (s *Scheduler) registerJobType(ctx context.Context, conn DB, jobTypeConfig *JobTypeConfig) error {
	var jobTypeID int32
	selectIDErr := conn.QueryRow(ctx, `select id from pgxjob_types where name = $1`, jobTypeConfig.Name).Scan(&jobTypeID)
	if errors.Is(selectIDErr, pgx.ErrNoRows) {
		_, insertErr := conn.Exec(ctx, `insert into pgxjob_types (name) values ($1) on conflict do nothing`, jobTypeConfig.Name)
		if insertErr != nil {
			return fmt.Errorf("failed to insert job type %s: %w", jobTypeConfig.Name, insertErr)
		}

		selectIDErr = conn.QueryRow(ctx, `select id from pgxjob_types where name = $1`, jobTypeConfig.Name).Scan(&jobTypeID)
	}
	if selectIDErr != nil {
		return fmt.Errorf("failed to select id for job type %s: %w", jobTypeConfig.Name, selectIDErr)
	}

	jt := &JobType{
		ID:           jobTypeID,
		Name:         jobTypeConfig.Name,
		DefaultGroup: s.jobGroupsByName[jobTypeConfig.DefaultGroupName],
		RunJob:       jobTypeConfig.RunJob,
	}

	s.jobTypesByName[jt.Name] = jt
	s.jobTypesByID[jt.ID] = jt

	return nil
}

type RunJobFunc func(ctx context.Context, job *Job) error

// UnmarshalParams returns a JobType.RunJob function that unmarshals job.Params into a T and calls fn.
func UnmarshalParams[T any](fn func(ctx context.Context, job *Job, params T) error) RunJobFunc {
	return func(ctx context.Context, job *Job) error {
		var params T
		err := json.Unmarshal(job.Params, &params)
		if err != nil {
			return fmt.Errorf("unmarshal job params failed: %w", err)
		}

		return fn(ctx, job, params)
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
	select {
	case <-m.setupDoneChan:
	case <-ctx.Done():
		return fmt.Errorf("pgxjob: schedule %w", ctx.Err())
	}

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
			`insert into pgxjob_asap_jobs (group_id, type_id, params, worker_id)
values ($1, $2, $3, (select id from pgxjob_workers where group_id = $1 order by random() limit 1))`,
			jobGroup.ID, jobType.ID, jobParams,
		)
		batch.Queue(`select pg_notify($1, $2)`, PGNotifyChannel, jobGroup.Name)
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

// Ctx returns the *Scheduler attached to ctx. If ctx does not have a *Scheduler attached then it returns
// DefaultContextScheduler.
func Ctx(ctx context.Context) *Scheduler {
	if s, ok := ctx.Value(ctxKey{}).(*Scheduler); ok {
		return s
	} else {
		return DefaultContextScheduler
	}
}

// WithContext returns a copy of ctx with s attached.
func (s *Scheduler) WithContext(ctx context.Context) context.Context {
	return context.WithValue(ctx, ctxKey{}, s)
}

// AcquireConnFunc is a function that acquires a database connection for exclusive use. It returns a release function
// that will be called when the connection is no longer needed.
type AcquireConnFunc func(ctx context.Context) (conn *pgxpool.Conn, release func(), err error)

// AcquireConnFuncFromPool returns an AcquireConnFunc that acquires connections from the given *pgxpool.Pool.
func AcquireConnFuncFromPool(pool *pgxpool.Pool) AcquireConnFunc {
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

	// ShouldLogJobRun is called for every job run. If it returns true then the run is logged to the pgxjob_job_runs
	// table. If it returns false it is not. If not set all job runs are logged.
	ShouldLogJobRun func(worker *Worker, job *Job, startTime, endTime time.Time, err error) bool

	minHeartbeatDelay                  time.Duration
	heartbeatDelayJitter               time.Duration
	workerDeadWithoutHeartbeatDuration time.Duration
}

type Worker struct {
	id int32

	config *WorkerConfig
	group  *JobGroup

	scheduler  *Scheduler
	signalChan chan struct{}

	cancelCtx context.Context
	cancel    context.CancelFunc

	startupCompleteChan chan struct{}

	mux                     sync.Mutex
	jobRunnerGoroutineCount int

	runningJobsMux sync.Mutex
	runningJobIDs  map[int64]struct{}

	jobRunnerGoroutineWaitGroup sync.WaitGroup
	jobChan                     chan *Job

	jobResultsChan          chan *jobResult
	writeJobResultsDoneChan chan struct{}

	heartbeatDoneChan         chan struct{}
	fetchAndStartJobsDoneChan chan struct{}
}

// StartWorker starts a worker. The *Worker is returned immediately, but the startup process is run in the background.
// This is to avoid blocking or returning an error if the database is temporarily unavailable. Use StartupComplete if
// it is necessary to wait for the worker to be ready.
func (m *Scheduler) StartWorker(config *WorkerConfig) (*Worker, error) {
	if config.GroupName == "" {
		config.GroupName = defaultGroupName
	}
	if !slices.Contains(m.config.JobGroups, config.GroupName) {
		return nil, fmt.Errorf("pgxjob: group with name %s not registered", config.GroupName)
	}

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

	if config.minHeartbeatDelay == 0 {
		config.minHeartbeatDelay = 45 * time.Second
	}
	if config.heartbeatDelayJitter == 0 {
		config.heartbeatDelayJitter = 30 * time.Second
	}
	if config.workerDeadWithoutHeartbeatDuration == 0 {
		config.workerDeadWithoutHeartbeatDuration = 5 * (config.minHeartbeatDelay + config.heartbeatDelayJitter)
	}

	w := &Worker{
		config:                    config,
		scheduler:                 m,
		signalChan:                make(chan struct{}, 1),
		startupCompleteChan:       make(chan struct{}),
		runningJobIDs:             make(map[int64]struct{}, config.MaxConcurrentJobs+config.MaxPrefetchedJobs),
		jobChan:                   make(chan *Job),
		jobResultsChan:            make(chan *jobResult),
		writeJobResultsDoneChan:   make(chan struct{}),
		heartbeatDoneChan:         make(chan struct{}),
		fetchAndStartJobsDoneChan: make(chan struct{}),
	}
	w.cancelCtx, w.cancel = context.WithCancel(context.Background())

	go func() {
		for {
			select {
			case <-w.cancelCtx.Done():
				return
			default:
			}

			err := w.start()
			if err == nil {
				go w.heartbeat()
				go w.writeJobResults()
				go w.fetchAndStartJobs()
				return
			}
			w.scheduler.handleError(fmt.Errorf("pgxjob: failed to setup worker: %w", err))

			select {
			case <-w.cancelCtx.Done():
				return
			case <-time.After(30 * time.Second):
			}
		}
	}()

	return w, nil
}

func (w *Worker) start() error {
	ctx, cancel := context.WithTimeout(w.cancelCtx, 30*time.Second)
	defer cancel()

	select {
	case <-w.scheduler.setupDoneChan:
	case <-ctx.Done():
		return ctx.Err()
	}

	w.group = w.scheduler.jobGroupsByName[w.config.GroupName]

	conn, release, err := w.scheduler.acquireConn(ctx)
	if err != nil {
		return fmt.Errorf("failed to get connection: %w", err)
	}
	defer release()

	w.id, err = pgxutil.SelectRow(ctx, conn,
		`insert into pgxjob_workers (heartbeat, group_id) values (now(), $1) returning id`,
		[]any{w.group.ID},
		pgx.RowTo[int32],
	)
	if err != nil {
		return fmt.Errorf("failed to insert into pgxjob_workers: %w", err)
	}

	close(w.startupCompleteChan)
	return nil
}

func (w *Worker) heartbeat() {
	defer close(w.heartbeatDoneChan)

	for {
		select {
		case <-w.cancelCtx.Done():
			return
		case <-time.After(w.config.minHeartbeatDelay + time.Duration(rand.Int63n(int64(w.config.heartbeatDelayJitter)))):
			func() {
				defer func() {
					if r := recover(); r != nil {
						w.handleWorkerError(fmt.Errorf("pgxjob: panic in heartbeat for %d: %v\n%s", w.id, r, debug.Stack()))
					}
				}()

				err := func() error {
					ctx, cancel := context.WithTimeout(w.cancelCtx, 15*time.Second)
					defer cancel()

					conn, release, err := w.scheduler.acquireConn(ctx)
					if err != nil {
						return fmt.Errorf("pgxjob: heartbeat for %d: failed to get connection: %w", w.id, err)
					}
					defer release()

					_, err = conn.Exec(ctx, `update pgxjob_workers set heartbeat = now() where id = $1`, w.id)
					if err != nil {
						return fmt.Errorf("pgxjob: heartbeat for %d: failed to update: %w", w.id, err)
					}

					_, err = conn.Exec(ctx, `delete from pgxjob_workers where heartbeat + $1 < now()`, w.config.workerDeadWithoutHeartbeatDuration)
					if err != nil {
						return fmt.Errorf("pgxjob: heartbeat for %d: failed to cleanup dead workers: %w", w.id, err)
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

	jobResults := make([]*jobResult, 0, w.config.MaxBufferedJobResults)
	asapJobIDsToDelete := make([]int64, 0, w.config.MaxBufferedJobResults)
	runAtJobIDsToDelete := make([]int64, 0, w.config.MaxBufferedJobResults)
	pgxjobJobRunsToInsert := make([]pgxjobJobRun, 0, w.config.MaxBufferedJobResults)

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
			w.runningJobsMux.Lock()
			for _, jr := range jobResults {
				delete(w.runningJobIDs, jr.job.ID)
			}
			w.runningJobsMux.Unlock()

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
			if w.config.ShouldLogJobRun == nil || w.config.ShouldLogJobRun(w, job, jobResult.startTime, jobResult.finishedAt, jobResult.err) {
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
					`update pgxjob_run_at_jobs set error_count = error_count + 1, last_error = $1, next_run_at = $2, worker_id = null where id = $3`,
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

		conn, release, err := w.scheduler.acquireConn(ctx)
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
		if len(jobResults) >= w.config.MaxBufferedJobResults {
			flush()
		} else if len(jobResults) == 1 {
			flushTimer.Reset(w.config.MaxBufferedJobResultAge)
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

// StartupComplete returns a channel that is closed when the worker start is complete.
func (w *Worker) StartupComplete() <-chan struct{} {
	return w.startupCompleteChan
}

// ID gets the worker's ID. This is only valid after the worker startup has completed. This is guaranteed while processing
// a job, but not immediately after StartWorker has returned. Use StartupComplete to wait for the worker to start.
func (w *Worker) ID() int32 {
	return w.id
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

		conn, release, err := w.scheduler.acquireConn(ctx)
		if err != nil {
			cleanupErrChan <- fmt.Errorf("pgxjob: shutdown failed to get connection for cleanup: %w", err)
			return
		}
		defer release()

		_, err = conn.Exec(ctx, `delete from pgxjob_workers where id = $1`, w.id)
		if err != nil {
			cleanupErrChan <- fmt.Errorf("pgxjob: shutdown failed to cleanup worker: %w", err)
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
		noJobsAvailableInDatabase := len(jobs) < w.config.MaxPrefetchedJobs
		w.runningJobsMux.Lock()
		for _, job := range jobs {
			w.runningJobIDs[job.ID] = struct{}{}
		}
		w.runningJobsMux.Unlock()

		for _, job := range jobs {
			select {
			case w.jobChan <- job:
			case <-w.cancelCtx.Done():
				return nil
			default:
				w.mux.Lock()
				if w.jobRunnerGoroutineCount < w.config.MaxConcurrentJobs {
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
			case <-time.NewTimer(w.config.PollInterval).C:
			case <-w.signalChan:
			}
		}
	}
}

// fetchPrelockedASAPJobsSQL is used to fetch pgxjob_asap_jobs that were prelocked by Schedule. It takes 4 bound
// parameters. $1 is group id. $2 is worker_id. $3 is an array of job IDs the worker is already working. $4 is the
// maximum number of jobs to fetch. ids.
const fetchPrelockedASAPJobsSQL = `select id, type_id, params, inserted_at
from pgxjob_asap_jobs
where group_id = $1
	and worker_id = $2
	and not id = any ($3)
limit $4`

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
	conn, release, err := w.scheduler.acquireConn(w.cancelCtx)
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

	runningJobIDs := []int64{} // Important to use empty slice instead of nil because of NULL behavior in SQL.
	w.runningJobsMux.Lock()
	for jobID := range w.runningJobIDs {
		runningJobIDs = append(runningJobIDs, jobID)
	}
	w.runningJobsMux.Unlock()

	rowToASAPJob := func(row pgx.CollectableRow) (*Job, error) {
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
	}

	jobs, err := pgxutil.Select(w.cancelCtx, conn,
		fetchPrelockedASAPJobsSQL,
		[]any{w.group.ID, w.id, runningJobIDs, w.config.MaxPrefetchedJobs},
		rowToASAPJob,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch prelocked pgxjob_asap_jobs: %w", err)
	}

	if len(jobs) < w.config.MaxPrefetchedJobs {
		asapJobs, err := pgxutil.Select(w.cancelCtx, conn,
			fetchAndLockASAPJobsSQL,
			[]any{w.group.ID, w.config.MaxPrefetchedJobs, w.id},
			rowToASAPJob,
		)
		if err != nil {
			// If some jobs were successfully locked. Return those without an error.
			if len(jobs) > 0 {
				return jobs, nil
			}
			return nil, fmt.Errorf("failed to fetch and lock pgxjob_asap_jobs: %w", err)
		}
		jobs = append(jobs, asapJobs...)
	}

	if len(jobs) < w.config.MaxPrefetchedJobs {
		runAtJobs, err := pgxutil.Select(w.cancelCtx, conn,
			fetchAndLockRunAtJobsSQL,
			[]any{w.group.ID, w.config.MaxPrefetchedJobs - len(jobs), w.id},
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
	w.scheduler.handleError(fmt.Errorf("worker %v: %w", w.id, err))
}

// Signal causes the worker to wake up and process requests. It is safe to call this from multiple goroutines. It does
// not block.
func (w *Worker) Signal() {
	select {
	case w.signalChan <- struct{}{}:
	default:
	}
}

// HandleNotification implements the pgxlisten.Handler interface. This allows a Worker to be used as a
// pgxlisten.Listener. When it receives a notification for the worker's job group it calls Signal.
func (w *Worker) HandleNotification(ctx context.Context, notification *pgconn.Notification, conn *pgx.Conn) error {
	if notification.Payload == w.group.Name {
		w.Signal()
	}

	return nil
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
func FilterError(runJob RunJobFunc, errorFilter ErrorFilter) RunJobFunc {
	return func(ctx context.Context, job *Job) error {
		jobErr := runJob(ctx, job)
		if jobErr != nil {
			return errorFilter.FilterError(job, jobErr)
		}

		return nil
	}
}

type ErrorFilter interface {
	FilterError(job *Job, jobErr error) error
}

type FilterErrorFunc func(job *Job, jobErr error) error

func (f FilterErrorFunc) FilterError(job *Job, jobErr error) error {
	return f(job, jobErr)
}

// RetryLinearBackoffErrorFilter is an ErrorFilter that returns an ErrorWithRetry if the job should be retried. It uses
// a linear backoff to determine when to schedule the retries.
type RetryLinearBackoffErrorFilter struct {
	// MaxRetries is the maximum number of times to retry.
	MaxRetries int32

	// BaseDelay is the amount of time to wait before the first retry. The wait time will increase by BaseDelay for each
	// retry.
	BaseDelay time.Duration
}

// FilterError returns an ErrorWithRetry if the job should be retried. If the error is already an ErrorWithRetry then it
// is returned unmodified. If the job should not be retried then the original error is returned.
func (f *RetryLinearBackoffErrorFilter) FilterError(job *Job, jobErr error) error {
	if jobErr == nil {
		return nil
	}

	if job.ErrorCount >= f.MaxRetries {
		return jobErr
	}

	var errorWithRetry *ErrorWithRetry
	if errors.As(jobErr, &errorWithRetry) {
		return jobErr
	}

	return &ErrorWithRetry{
		Err:     jobErr,
		RetryAt: time.Now().Add(time.Duration(job.ErrorCount+1) * f.BaseDelay),
	}
}

// LogFinalJobRuns is a ShouldLogJobRun function that returns true for the final run of a job. That is, the run was
// successful or it failed and will not try again.
func LogFinalJobRuns(worker *Worker, job *Job, startTime, endTime time.Time, err error) bool {
	if err == nil {
		return true
	}

	var errorWithRetry *ErrorWithRetry
	return !errors.As(err, &errorWithRetry)
}

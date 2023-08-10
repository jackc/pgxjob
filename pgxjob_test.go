package pgxjob_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgxjob"
	"github.com/jackc/pgxutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// mustConnect connects to the database specified by the PGXJOB_TEST_DATABASE environment variable. It automatically
// closes the connection when the test is finished.
func mustConnect(t testing.TB) *pgx.Conn {
	t.Helper()

	dbname := os.Getenv("PGXJOB_TEST_DATABASE")
	if dbname == "" {
		t.Fatal("PGXJOB_TEST_DATABASE environment variable must be set")
	}

	config, err := pgx.ParseConfig(fmt.Sprintf("dbname=%s", os.Getenv("PGXJOB_TEST_DATABASE")))
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	conn, err := pgx.ConnectConfig(ctx, config)
	require.NoError(t, err)

	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		err := conn.Close(ctx)
		if err != nil {
			t.Logf("Warning: error closing connection: %v", err)
		}
	})

	return conn
}

func mustNewDBPool(t testing.TB) *pgxpool.Pool {
	t.Helper()

	dbname := os.Getenv("PGXJOB_TEST_DATABASE")
	if dbname == "" {
		t.Fatal("PGXJOB_TEST_DATABASE environment variable must be set")
	}

	config, err := pgxpool.ParseConfig(fmt.Sprintf("dbname=%s", os.Getenv("PGXJOB_TEST_DATABASE")))
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	pool, err := pgxpool.NewWithConfig(ctx, config)
	require.NoError(t, err)

	t.Cleanup(func() {
		// Close pool in a goroutine to avoid blocking forever if there are connections checked out.
		go pool.Close()
	})

	return pool
}

func mustCleanDatabase(t testing.TB, conn *pgx.Conn) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	for _, table := range []string{
		"pgxjob_workers",
		"pgxjob_asap_jobs",
		"pgxjob_run_at_jobs",
		"pgxjob_job_runs",
	} {
		_, err := conn.Exec(ctx, fmt.Sprintf("delete from %s", table))
		require.NoErrorf(t, err, "error cleaning table %s", table)
	}
}

type asapJob struct {
	ID         int64
	InsertedAt time.Time
	GroupID    int32
	TypeID     int32
	WorkerID   pgtype.Int4
	Params     []byte
}

type runAtJob struct {
	ID         int64
	InsertedAt time.Time
	RunAt      pgtype.Timestamptz
	NextRunAt  pgtype.Timestamptz
	GroupID    int32
	TypeID     int32
	WorkerID   pgtype.Int4
	ErrorCount pgtype.Int4
	LastError  pgtype.Text
	Params     []byte
}

type jobRun struct {
	JobID      int64
	InsertedAt time.Time
	RunAt      time.Time
	StartedAt  time.Time
	FinishedAt time.Time
	RunNumber  int32
	GroupID    int32
	TypeID     int32
	Params     []byte
	LastError  pgtype.Text
}

func TestASAPEndToEnd(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	startTime := time.Now()

	conn := mustConnect(t)
	mustCleanDatabase(t, conn)
	dbpool := mustNewDBPool(t)

	jobRanChan := make(chan struct{})
	scheduler, err := pgxjob.NewScheduler(ctx, pgxjob.GetConnFromPoolFunc(dbpool))
	require.NoError(t, err)

	err = scheduler.RegisterJobType(ctx, pgxjob.RegisterJobTypeParams{
		Name: "test",
		RunJob: func(ctx context.Context, job *pgxjob.Job) error {
			jobRanChan <- struct{}{}
			return nil
		},
	})
	require.NoError(t, err)

	err = scheduler.ScheduleNow(ctx, conn, "test", nil)
	require.NoError(t, err)

	afterScheduleNow := time.Now()

	job, err := pgxutil.SelectRow(ctx, conn, `select * from pgxjob_asap_jobs`, nil, pgx.RowToStructByPos[asapJob])
	require.NoError(t, err)

	require.True(t, job.InsertedAt.After(startTime))
	require.True(t, job.InsertedAt.Before(afterScheduleNow))

	defaultGroupID, err := pgxutil.SelectRow(ctx, conn, `select id from pgxjob_groups where name = 'default'`, nil, pgx.RowTo[int32])
	require.NoError(t, err)
	require.Equal(t, defaultGroupID, job.GroupID)

	testJobTypeID, err := pgxutil.SelectRow(ctx, conn, `select id from pgxjob_types where name = 'test'`, nil, pgx.RowTo[int32])
	require.NoError(t, err)
	require.Equal(t, testJobTypeID, job.TypeID)

	require.Equal(t, []byte(nil), job.Params)

	worker, err := scheduler.NewWorker(ctx, pgxjob.WorkerConfig{
		HandleWorkerError: func(worker *pgxjob.Worker, err error) {
			t.Errorf("worker error: %v", err)
		},
	})
	require.NoError(t, err)

	startErrChan := make(chan error)
	go func() {
		err := worker.Start()
		startErrChan <- err
	}()

	select {
	case <-jobRanChan:
	case <-time.After(30 * time.Second):
		t.Fatal("timed out waiting for job to run")
	}

	worker.Shutdown(context.Background())

	err = <-startErrChan
	require.NoError(t, err)

	afterRunNow := time.Now()

	jobRun, err := pgxutil.SelectRow(ctx, conn, `select * from pgxjob_job_runs where job_id = $1`, []any{job.ID}, pgx.RowToStructByPos[jobRun])
	require.NoError(t, err)

	require.Equal(t, job.ID, jobRun.JobID)
	require.True(t, jobRun.InsertedAt.Equal(job.InsertedAt))
	require.True(t, jobRun.RunAt.Equal(jobRun.InsertedAt))
	require.True(t, jobRun.StartedAt.After(startTime))
	require.True(t, jobRun.StartedAt.Before(afterRunNow))
	require.True(t, jobRun.FinishedAt.After(startTime))
	require.True(t, jobRun.FinishedAt.Before(afterRunNow))
	require.EqualValues(t, 1, jobRun.RunNumber)
	require.Equal(t, job.GroupID, jobRun.GroupID)
	require.Equal(t, job.TypeID, jobRun.TypeID)
	require.Equal(t, job.Params, jobRun.Params)
	require.False(t, jobRun.LastError.Valid)
}

func TestRunAtEndToEnd(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	startTime := time.Now()

	conn := mustConnect(t)
	mustCleanDatabase(t, conn)
	dbpool := mustNewDBPool(t)

	jobRanChan := make(chan struct{})
	scheduler, err := pgxjob.NewScheduler(ctx, pgxjob.GetConnFromPoolFunc(dbpool))
	require.NoError(t, err)

	err = scheduler.RegisterJobType(ctx, pgxjob.RegisterJobTypeParams{
		Name: "test",
		RunJob: func(ctx context.Context, job *pgxjob.Job) error {
			jobRanChan <- struct{}{}
			return nil
		},
	})
	require.NoError(t, err)

	runAt := time.Now().Add(100 * time.Millisecond).Truncate(time.Millisecond) // Truncate because PostgreSQL only supports microsecond precision.
	err = scheduler.Schedule(ctx, conn, "test", nil, pgxjob.JobSchedule{RunAt: runAt})
	require.NoError(t, err)

	afterScheduleNow := time.Now()

	job, err := pgxutil.SelectRow(ctx, conn, `select * from pgxjob_run_at_jobs`, nil, pgx.RowToStructByPos[runAtJob])
	require.NoError(t, err)

	require.True(t, job.InsertedAt.After(startTime))
	require.True(t, job.InsertedAt.Before(afterScheduleNow))
	require.True(t, job.RunAt.Time.Equal(runAt))
	require.True(t, job.NextRunAt.Time.Equal(runAt))

	defaultGroupID, err := pgxutil.SelectRow(ctx, conn, `select id from pgxjob_groups where name = 'default'`, nil, pgx.RowTo[int32])
	require.NoError(t, err)
	require.Equal(t, defaultGroupID, job.GroupID)

	testJobTypeID, err := pgxutil.SelectRow(ctx, conn, `select id from pgxjob_types where name = 'test'`, nil, pgx.RowTo[int32])
	require.NoError(t, err)
	require.Equal(t, testJobTypeID, job.TypeID)

	require.Equal(t, []byte(nil), job.Params)

	worker, err := scheduler.NewWorker(ctx, pgxjob.WorkerConfig{
		PollInterval: 50 * time.Millisecond,
		HandleWorkerError: func(worker *pgxjob.Worker, err error) {
			t.Errorf("worker error: %v", err)
		},
	})
	require.NoError(t, err)

	startErrChan := make(chan error)
	go func() {
		err := worker.Start()
		startErrChan <- err
	}()

	select {
	case <-jobRanChan:
	case <-time.After(30 * time.Second):
		t.Fatal("timed out waiting for job to run")
	}

	worker.Shutdown(context.Background())

	err = <-startErrChan
	require.NoError(t, err)

	afterRunNow := time.Now()

	jobRun, err := pgxutil.SelectRow(ctx, conn, `select * from pgxjob_job_runs where job_id = $1`, []any{job.ID}, pgx.RowToStructByPos[jobRun])
	require.NoError(t, err)

	require.Equal(t, job.ID, jobRun.JobID)
	require.True(t, jobRun.InsertedAt.Equal(job.InsertedAt))
	require.True(t, jobRun.RunAt.Equal(runAt))
	require.True(t, jobRun.StartedAt.After(startTime))
	require.True(t, jobRun.StartedAt.Before(afterRunNow))
	require.True(t, jobRun.FinishedAt.After(startTime))
	require.True(t, jobRun.FinishedAt.Before(afterRunNow))
	require.EqualValues(t, 1, jobRun.RunNumber)
	require.Equal(t, job.GroupID, jobRun.GroupID)
	require.Equal(t, job.TypeID, jobRun.TypeID)
	require.Equal(t, job.Params, jobRun.Params)
	require.False(t, jobRun.LastError.Valid)
}

func TestConcurrentJobSchedulingAndWorking(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	conn := mustConnect(t)
	mustCleanDatabase(t, conn)
	dbpool := mustNewDBPool(t)

	jobRanChan := make(chan struct{})
	scheduler, err := pgxjob.NewScheduler(ctx, pgxjob.GetConnFromPoolFunc(dbpool))
	require.NoError(t, err)

	err = scheduler.RegisterJobType(ctx, pgxjob.RegisterJobTypeParams{
		Name: "test",
		RunJob: func(ctx context.Context, job *pgxjob.Job) error {
			jobRanChan <- struct{}{}
			return nil
		},
	})
	require.NoError(t, err)

	totalJobs := 0

	// Schedule an ASAP job before the worker has started.
	err = scheduler.ScheduleNow(ctx, conn, "test", nil)
	require.NoError(t, err)
	totalJobs++

	// Schedule a Run At job before the worker has started.
	err = scheduler.Schedule(ctx, conn, "test", nil, pgxjob.JobSchedule{RunAt: time.Now().Add(500 * time.Millisecond)})
	require.NoError(t, err)
	totalJobs++

	worker, err := scheduler.NewWorker(ctx, pgxjob.WorkerConfig{
		PollInterval: 50 * time.Millisecond,
		HandleWorkerError: func(worker *pgxjob.Worker, err error) {
			t.Errorf("worker error: %v", err)
		},
	})
	require.NoError(t, err)

	startErrChan := make(chan error)
	go func() {
		err := worker.Start()
		startErrChan <- err
	}()

	for i := 0; i < 10; i++ {
		time.Sleep(10 * time.Millisecond)
		err = scheduler.ScheduleNow(ctx, conn, "test", nil)
		require.NoError(t, err)
		totalJobs++

		err = scheduler.Schedule(ctx, conn, "test", nil, pgxjob.JobSchedule{RunAt: time.Now().Add(500 * time.Millisecond)})
		require.NoError(t, err)
		totalJobs++
	}

	for i := 0; i < totalJobs; i++ {
		select {
		case <-jobRanChan:
		case <-time.After(30 * time.Second):
			t.Fatal("timed out waiting for job to run")
		}
	}

	worker.Shutdown(context.Background())

	err = <-startErrChan
	require.NoError(t, err)

	pendingASAPJobsCount, err := pgxutil.SelectRow(ctx, conn, `select count(*) from pgxjob_asap_jobs`, nil, pgx.RowTo[int32])
	require.NoError(t, err)
	require.EqualValues(t, 0, pendingASAPJobsCount)

	pendingRunAtJobsCount, err := pgxutil.SelectRow(ctx, conn, `select count(*) from pgxjob_run_at_jobs`, nil, pgx.RowTo[int32])
	require.NoError(t, err)
	require.EqualValues(t, 0, pendingRunAtJobsCount)

	jobRunsCount, err := pgxutil.SelectRow(ctx, conn, `select count(*) from pgxjob_job_runs`, nil, pgx.RowTo[int32])
	require.NoError(t, err)
	require.EqualValues(t, totalJobs, jobRunsCount)
}

func TestJobFailedNoRetry(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	startTime := time.Now()

	conn := mustConnect(t)
	mustCleanDatabase(t, conn)
	dbpool := mustNewDBPool(t)

	jobRanChan := make(chan struct{})
	scheduler, err := pgxjob.NewScheduler(ctx, pgxjob.GetConnFromPoolFunc(dbpool))
	require.NoError(t, err)

	err = scheduler.RegisterJobType(ctx, pgxjob.RegisterJobTypeParams{
		Name: "test",
		RunJob: func(ctx context.Context, job *pgxjob.Job) error {
			jobRanChan <- struct{}{}
			return fmt.Errorf("test error")
		},
	})
	require.NoError(t, err)

	err = scheduler.ScheduleNow(ctx, conn, "test", nil)
	require.NoError(t, err)

	job, err := pgxutil.SelectRow(ctx, conn, `select * from pgxjob_asap_jobs`, nil, pgx.RowToStructByPos[asapJob])
	require.NoError(t, err)

	worker, err := scheduler.NewWorker(ctx, pgxjob.WorkerConfig{
		HandleWorkerError: func(worker *pgxjob.Worker, err error) {
			t.Errorf("worker error: %v", err)
		},
	})
	require.NoError(t, err)

	startErrChan := make(chan error)
	go func() {
		err := worker.Start()
		startErrChan <- err
	}()

	select {
	case <-jobRanChan:
	case <-time.After(30 * time.Second):
		t.Fatal("timed out waiting for job to run")
	}

	worker.Shutdown(context.Background())

	err = <-startErrChan
	require.NoError(t, err)

	afterRunNow := time.Now()

	_, err = pgxutil.SelectRow(ctx, conn, `select * from pgxjob_asap_jobs where id = $1`, []any{job.ID}, pgx.RowToStructByPos[asapJob])
	require.Error(t, err)
	require.ErrorIs(t, err, pgx.ErrNoRows)

	_, err = pgxutil.SelectRow(ctx, conn, `select * from pgxjob_run_at_jobs where id = $1`, []any{job.ID}, pgx.RowToStructByPos[runAtJob])
	require.Error(t, err)
	require.ErrorIs(t, err, pgx.ErrNoRows)

	jobRun, err := pgxutil.SelectRow(ctx, conn, `select * from pgxjob_job_runs where job_id = $1`, []any{job.ID}, pgx.RowToStructByPos[jobRun])
	require.NoError(t, err)

	require.Equal(t, job.ID, jobRun.JobID)
	require.True(t, jobRun.InsertedAt.Equal(job.InsertedAt))
	require.True(t, jobRun.RunAt.Equal(jobRun.InsertedAt))
	require.True(t, jobRun.StartedAt.After(startTime))
	require.True(t, jobRun.StartedAt.Before(afterRunNow))
	require.True(t, jobRun.FinishedAt.After(startTime))
	require.True(t, jobRun.FinishedAt.Before(afterRunNow))
	require.EqualValues(t, 1, jobRun.RunNumber)
	require.Equal(t, job.GroupID, jobRun.GroupID)
	require.Equal(t, job.TypeID, jobRun.TypeID)
	require.Equal(t, job.Params, jobRun.Params)
	require.Equal(t, "test error", jobRun.LastError.String)
}

func TestUnknownJobType(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	conn := mustConnect(t)
	mustCleanDatabase(t, conn)
	dbpool := mustNewDBPool(t)

	scheduler, err := pgxjob.NewScheduler(ctx, pgxjob.GetConnFromPoolFunc(dbpool))
	require.NoError(t, err)

	err = scheduler.RegisterJobType(ctx, pgxjob.RegisterJobTypeParams{
		Name: "test",
		RunJob: func(ctx context.Context, job *pgxjob.Job) error {
			return nil
		},
	})
	require.NoError(t, err)

	worker, err := scheduler.NewWorker(ctx, pgxjob.WorkerConfig{
		HandleWorkerError: func(worker *pgxjob.Worker, err error) {
			t.Errorf("worker error: %v", err)
		},
	})
	require.NoError(t, err)

	err = pgxutil.InsertRow(ctx, conn, "pgxjob_asap_jobs", map[string]any{
		"group_id": 1,  // 1 should always be the default group
		"type_id":  -1, // -1 should never exist
	})
	require.NoError(t, err)

	startErrChan := make(chan error)
	go func() {
		err := worker.Start()
		startErrChan <- err
	}()

	require.Eventually(t, func() bool {
		n, err := pgxutil.SelectRow(ctx, conn, `select count(*) from pgxjob_asap_jobs`, nil, pgx.RowTo[int32])
		require.NoError(t, err)
		return n == 0
	}, 5*time.Second, 100*time.Millisecond)

	worker.Shutdown(context.Background())

	err = <-startErrChan
	require.NoError(t, err)

	jobRun, err := pgxutil.SelectRow(ctx, conn, `select * from pgxjob_job_runs`, nil, pgx.RowToStructByPos[jobRun])
	require.NoError(t, err)

	require.EqualValues(t, 1, jobRun.RunNumber)
	require.Equal(t, "pgxjob: job type with id -1 not registered", jobRun.LastError.String)
}

func TestJobFailedErrorWithRetry(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	startTime := time.Now()

	conn := mustConnect(t)
	mustCleanDatabase(t, conn)
	dbpool := mustNewDBPool(t)

	jobRanChan := make(chan struct{})
	scheduler, err := pgxjob.NewScheduler(ctx, pgxjob.GetConnFromPoolFunc(dbpool))
	require.NoError(t, err)

	retryAt := time.Now().Add(1 * time.Hour)

	err = scheduler.RegisterJobType(ctx, pgxjob.RegisterJobTypeParams{
		Name: "test",
		RunJob: func(ctx context.Context, job *pgxjob.Job) error {
			jobRanChan <- struct{}{}
			return &pgxjob.ErrorWithRetry{Err: fmt.Errorf("test error"), RetryAt: retryAt}
		},
	})
	require.NoError(t, err)

	err = scheduler.ScheduleNow(ctx, conn, "test", nil)
	require.NoError(t, err)

	worker, err := scheduler.NewWorker(ctx, pgxjob.WorkerConfig{
		HandleWorkerError: func(worker *pgxjob.Worker, err error) {
			t.Errorf("worker error: %v", err)
		},
	})
	require.NoError(t, err)

	startErrChan := make(chan error)
	go func() {
		err := worker.Start()
		startErrChan <- err
	}()

	select {
	case <-jobRanChan:
	case <-time.After(30 * time.Second):
		t.Fatal("timed out waiting for job to run")
	}

	worker.Shutdown(context.Background())

	err = <-startErrChan
	require.NoError(t, err)

	afterRunNow := time.Now()

	job, err := pgxutil.SelectRow(ctx, conn, `select * from pgxjob_run_at_jobs`, nil, pgx.RowToStructByPos[runAtJob])
	require.NoError(t, err)
	require.EqualValues(t, 1, job.ErrorCount.Int32)
	require.Equal(t, "test error", job.LastError.String)

	jobRun, err := pgxutil.SelectRow(ctx, conn, `select * from pgxjob_job_runs where job_id = $1`, []any{job.ID}, pgx.RowToStructByPos[jobRun])
	require.NoError(t, err)

	require.Equal(t, job.ID, jobRun.JobID)
	require.True(t, jobRun.InsertedAt.Equal(job.InsertedAt))
	require.True(t, jobRun.RunAt.Equal(jobRun.InsertedAt))
	require.True(t, jobRun.StartedAt.After(startTime))
	require.True(t, jobRun.StartedAt.Before(afterRunNow))
	require.True(t, jobRun.FinishedAt.After(startTime))
	require.True(t, jobRun.FinishedAt.Before(afterRunNow))
	require.EqualValues(t, 1, jobRun.RunNumber)
	require.Equal(t, job.GroupID, jobRun.GroupID)
	require.Equal(t, job.TypeID, jobRun.TypeID)
	require.Equal(t, job.Params, jobRun.Params)
	require.Equal(t, job.LastError, jobRun.LastError)
}

func TestWorkerRunsBacklog(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	conn := mustConnect(t)
	mustCleanDatabase(t, conn)
	dbpool := mustNewDBPool(t)

	jobRanChan := make(chan struct{})
	scheduler, err := pgxjob.NewScheduler(ctx, pgxjob.GetConnFromPoolFunc(dbpool))
	require.NoError(t, err)

	err = scheduler.RegisterJobType(ctx, pgxjob.RegisterJobTypeParams{
		Name: "test",
		RunJob: func(ctx context.Context, job *pgxjob.Job) error {
			jobRanChan <- struct{}{}
			return nil
		},
	})
	require.NoError(t, err)

	backlogCount := 10000
	for i := 0; i < backlogCount; i++ {
		err = scheduler.ScheduleNow(ctx, conn, "test", nil)
		require.NoError(t, err)
	}

	worker, err := scheduler.NewWorker(ctx, pgxjob.WorkerConfig{
		HandleWorkerError: func(worker *pgxjob.Worker, err error) {
			t.Errorf("worker error: %v", err)
		},
	})
	require.NoError(t, err)

	startErrChan := make(chan error)
	go func() {
		err := worker.Start()
		startErrChan <- err
	}()

	for i := 0; i < backlogCount; i++ {
		select {
		case <-jobRanChan:
		case <-ctx.Done():
			t.Fatal("timed out waiting for job to run")
		}
	}

	worker.Shutdown(context.Background())

	err = <-startErrChan
	require.NoError(t, err)

	jobsStillPending, err := pgxutil.SelectRow(ctx, conn,
		`select (select count(*) from pgxjob_asap_jobs) + (select count(*) from pgxjob_run_at_jobs)`,
		nil,
		pgx.RowTo[int32],
	)
	require.NoError(t, err)
	require.EqualValues(t, 0, jobsStillPending)

	jobsRun, err := pgxutil.SelectRow(ctx, conn, `select count(*) from pgxjob_job_runs`, nil, pgx.RowTo[int32])
	require.NoError(t, err)
	require.EqualValues(t, backlogCount, jobsRun)
}

func TestWorkerIgnoresOtherJobGroups(t *testing.T) {
	// This test takes a while because it is waiting for something *not* to happen.
	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	conn := mustConnect(t)
	mustCleanDatabase(t, conn)
	dbpool := mustNewDBPool(t)

	scheduler, err := pgxjob.NewScheduler(ctx, pgxjob.GetConnFromPoolFunc(dbpool))
	require.NoError(t, err)

	err = scheduler.RegisterJobGroup(ctx, "other")
	require.NoError(t, err)

	err = scheduler.RegisterJobType(ctx, pgxjob.RegisterJobTypeParams{
		Name: "test",
		RunJob: func(ctx context.Context, job *pgxjob.Job) error {
			return nil
		},
	})
	require.NoError(t, err)

	err = scheduler.ScheduleNow(ctx, conn, "test", nil)
	require.NoError(t, err)

	err = scheduler.Schedule(ctx, conn, "test", nil, pgxjob.JobSchedule{GroupName: "other"})
	require.NoError(t, err)

	worker, err := scheduler.NewWorker(ctx, pgxjob.WorkerConfig{
		PollInterval: 500 * time.Millisecond,
		HandleWorkerError: func(worker *pgxjob.Worker, err error) {
			t.Errorf("worker error: %v", err)
		},
	})
	require.NoError(t, err)

	startErrChan := make(chan error)
	go func() {
		err := worker.Start()
		startErrChan <- err
	}()

	time.Sleep(5 * time.Second)

	worker.Shutdown(context.Background())

	err = <-startErrChan
	require.NoError(t, err)

	// Our job did run.
	jobsRun, err := pgxutil.SelectRow(ctx, conn, `select count(*) from pgxjob_job_runs`, nil, pgx.RowTo[int32])
	require.NoError(t, err)
	require.EqualValues(t, 1, jobsRun)

	// But the other job did not.
	asapJob, err := pgxutil.SelectRow(ctx, conn, `select * from pgxjob_asap_jobs`, nil, pgx.RowToAddrOfStructByPos[asapJob])
	require.NoError(t, err)

	otherGroupID, err := pgxutil.SelectRow(ctx, conn, `select id from pgxjob_groups where name = $1`, []any{"other"}, pgx.RowTo[int32])
	require.NoError(t, err)

	require.EqualValues(t, otherGroupID, asapJob.GroupID)
}

func TestWorkerShutdown(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	conn := mustConnect(t)
	mustCleanDatabase(t, conn)
	dbpool := mustNewDBPool(t)

	jobRanChan := make(chan struct{})
	scheduler, err := pgxjob.NewScheduler(ctx, pgxjob.GetConnFromPoolFunc(dbpool))
	require.NoError(t, err)

	err = scheduler.RegisterJobType(ctx, pgxjob.RegisterJobTypeParams{
		Name: "test",
		RunJob: func(ctx context.Context, job *pgxjob.Job) error {
			select {
			case jobRanChan <- struct{}{}:
			case <-time.After(10 * time.Millisecond):
			}
			return nil
		},
	})
	require.NoError(t, err)

	asapBacklogCount := 600
	for i := 0; i < asapBacklogCount; i++ {
		err = scheduler.ScheduleNow(ctx, conn, "test", nil)
		require.NoError(t, err)
	}
	runAtBacklogCount := 600
	for i := 0; i < runAtBacklogCount; i++ {
		err = scheduler.Schedule(ctx, conn, "test", nil, pgxjob.JobSchedule{RunAt: time.Now().Add(-1 * time.Second)})
		require.NoError(t, err)
	}

	worker, err := scheduler.NewWorker(ctx, pgxjob.WorkerConfig{
		MaxConcurrentJobs: 1,
		MaxPrefetchedJobs: 1000,
		HandleWorkerError: func(worker *pgxjob.Worker, err error) {
			t.Errorf("worker error: %v", err)
		},
	})
	require.NoError(t, err)

	startErrChan := make(chan error)
	go func() {
		err := worker.Start()
		startErrChan <- err
	}()

	// Wait for at least 10 jobs to have started/run.
	for i := 0; i < 10; i++ {
		select {
		case <-jobRanChan:
		case <-ctx.Done():
			t.Fatal("timed out waiting for job to run")
		}
	}

	worker.Shutdown(context.Background())

	err = <-startErrChan
	require.NoError(t, err)

	shutdownWorkerExists, err := pgxutil.SelectRow(ctx, conn, `select exists(select id from pgxjob_workers where id = $1)`, []any{worker.ID}, pgx.RowTo[bool])
	require.NoError(t, err)
	require.Falsef(t, shutdownWorkerExists, "shutdown worker still exists")

	lockedASAPJobs, err := pgxutil.SelectRow(ctx, conn,
		`select count(*) from pgxjob_asap_jobs where worker_id is not null`,
		nil,
		pgx.RowTo[int32],
	)
	require.NoError(t, err)
	require.EqualValues(t, 0, lockedASAPJobs)

	lockedRunAtJobs, err := pgxutil.SelectRow(ctx, conn,
		`select count(*) from pgxjob_run_at_jobs where worker_id is not null`,
		nil,
		pgx.RowTo[int32],
	)
	require.NoError(t, err)
	require.EqualValues(t, 0, lockedRunAtJobs)

	unlockedASAPJobs, err := pgxutil.SelectRow(ctx, conn,
		`select count(*) from pgxjob_asap_jobs where worker_id is null`,
		nil,
		pgx.RowTo[int32],
	)
	require.NoError(t, err)

	unlockedRunAtJobs, err := pgxutil.SelectRow(ctx, conn,
		`select count(*) from pgxjob_run_at_jobs where worker_id is null`,
		nil,
		pgx.RowTo[int32],
	)
	require.NoError(t, err)

	jobsRun, err := pgxutil.SelectRow(ctx, conn, `select count(*) from pgxjob_job_runs`, nil, pgx.RowTo[int32])
	require.NoError(t, err)

	require.EqualValues(t, asapBacklogCount+runAtBacklogCount, unlockedASAPJobs+unlockedRunAtJobs+jobsRun)

}

func TestWorkerHeartbeatBeats(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	conn := mustConnect(t)
	mustCleanDatabase(t, conn)
	dbpool := mustNewDBPool(t)

	scheduler, err := pgxjob.NewScheduler(ctx, pgxjob.GetConnFromPoolFunc(dbpool))
	require.NoError(t, err)

	err = scheduler.RegisterJobType(ctx, pgxjob.RegisterJobTypeParams{
		Name: "test",
		RunJob: func(ctx context.Context, job *pgxjob.Job) error {
			return nil
		},
	})
	require.NoError(t, err)

	worker, err := scheduler.NewWorker(ctx, pgxjob.WorkerConfig{
		HandleWorkerError: func(worker *pgxjob.Worker, err error) {
			t.Errorf("worker error: %v", err)
		},
	})
	require.NoError(t, err)

	worker.SetMinHeartbeatDelayForTest(50 * time.Millisecond)
	worker.SetHeartbeatDelayJitterForTest(50 * time.Millisecond)

	firstHeartbeat, err := pgxutil.SelectRow(ctx, conn, `select heartbeat from pgxjob_workers where id = $1`, []any{worker.ID}, pgx.RowTo[time.Time])
	require.NoError(t, err)

	startErrChan := make(chan error)
	go func() {
		err := worker.Start()
		startErrChan <- err
	}()

	require.Eventually(t, func() bool {
		heartbeat, err := pgxutil.SelectRow(ctx, conn, `select heartbeat from pgxjob_workers where id = $1`, []any{worker.ID}, pgx.RowTo[time.Time])
		require.NoError(t, err)
		return heartbeat.After(firstHeartbeat)
	}, 5*time.Second, 100*time.Millisecond)

	worker.Shutdown(context.Background())

	err = <-startErrChan
	require.NoError(t, err)
}

func TestWorkerHeartbeatCleansUpDeadWorkers(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	conn := mustConnect(t)
	mustCleanDatabase(t, conn)
	dbpool := mustNewDBPool(t)

	scheduler, err := pgxjob.NewScheduler(ctx, pgxjob.GetConnFromPoolFunc(dbpool))
	require.NoError(t, err)

	err = scheduler.RegisterJobType(ctx, pgxjob.RegisterJobTypeParams{
		Name: "test",
		RunJob: func(ctx context.Context, job *pgxjob.Job) error {
			return nil
		},
	})
	require.NoError(t, err)

	err = scheduler.ScheduleNow(ctx, conn, "test", nil)
	require.NoError(t, err)

	err = scheduler.Schedule(ctx, conn, "test", nil, pgxjob.JobSchedule{RunAt: time.Now().Add(-30 * time.Minute)})
	require.NoError(t, err)

	deadWorkerID, err := pgxutil.InsertRowReturning(ctx, conn,
		"pgxjob_workers",
		map[string]any{"heartbeat": time.Now().Add(-time.Hour)},
		"id",
		pgx.RowTo[int32],
	)
	require.NoError(t, err)

	err = pgxutil.UpdateRow(ctx, conn,
		"pgxjob_asap_jobs",
		map[string]any{"inserted_at": time.Now().Add(-time.Hour), "worker_id": deadWorkerID},
		nil,
	)
	require.NoError(t, err)

	err = pgxutil.UpdateRow(ctx, conn,
		"pgxjob_run_at_jobs",
		map[string]any{"inserted_at": time.Now().Add(-time.Hour), "worker_id": deadWorkerID},
		nil,
	)
	require.NoError(t, err)

	worker, err := scheduler.NewWorker(ctx, pgxjob.WorkerConfig{
		PollInterval: 500 * time.Millisecond,
		HandleWorkerError: func(worker *pgxjob.Worker, err error) {
			t.Errorf("worker error: %v", err)
		},
	})
	require.NoError(t, err)

	worker.SetMinHeartbeatDelayForTest(50 * time.Millisecond)
	worker.SetHeartbeatDelayJitterForTest(50 * time.Millisecond)

	startErrChan := make(chan error)
	go func() {
		err := worker.Start()
		startErrChan <- err
	}()

	require.EventuallyWithT(t, func(c *assert.CollectT) {
		deadWorkerExists, err := pgxutil.SelectRow(ctx, conn, `select exists(select id from pgxjob_workers where id = $1)`, []any{deadWorkerID}, pgx.RowTo[bool])
		assert.NoError(c, err)
		assert.Falsef(c, deadWorkerExists, "dead worker still exists")

		asapJobsStillPending, err := pgxutil.SelectRow(ctx, conn,
			`select count(*) from pgxjob_asap_jobs`,
			nil,
			pgx.RowTo[int32],
		)
		assert.NoError(c, err)
		assert.EqualValuesf(c, 0, asapJobsStillPending, "asap jobs still pending")

		runAtJobsStillPending, err := pgxutil.SelectRow(ctx, conn,
			`select count(*) from pgxjob_run_at_jobs`,
			nil,
			pgx.RowTo[int32],
		)
		assert.NoError(c, err)
		assert.EqualValuesf(c, 0, runAtJobsStillPending, "run at jobs still pending")

		jobsRun, err := pgxutil.SelectRow(ctx, conn, `select count(*) from pgxjob_job_runs`, nil, pgx.RowTo[int32])
		assert.NoError(c, err)
		assert.EqualValues(c, 2, jobsRun)
	}, 5*time.Second, 100*time.Millisecond)

	worker.Shutdown(context.Background())

	err = <-startErrChan
	require.NoError(t, err)
}

func TestUnmarshalParams(t *testing.T) {
	type T struct {
		Foo string
		Bar int
	}

	fn := pgxjob.UnmarshalParams(func(ctx context.Context, job *pgxjob.Job, params *T) error {
		require.Equal(t, "foo", params.Foo)
		require.Equal(t, 123, params.Bar)
		return nil
	})

	err := fn(context.Background(), &pgxjob.Job{Params: []byte(`{"foo":"foo","bar":123}`)})
	require.NoError(t, err)
}

func TestFilterError(t *testing.T) {
	var originalError error
	fn := pgxjob.FilterError(func(ctx context.Context, job *pgxjob.Job) error {
		return originalError
	}, func(job *pgxjob.Job, jobErr error) error {
		return fmt.Errorf("filtered error")
	})

	err := fn(context.Background(), nil)
	require.NoError(t, err)

	originalError = fmt.Errorf("original error")
	err = fn(context.Background(), nil)
	require.EqualError(t, err, "filtered error")
}

func TestRetryLinearBackoff(t *testing.T) {
	var originalError error
	fn := pgxjob.RetryLinearBackoff(func(ctx context.Context, job *pgxjob.Job) error {
		return originalError
	}, 3, 1*time.Hour)

	for i, tt := range []struct {
		originalError error
		errorCount    int32
		retryDelay    time.Duration
	}{
		{nil, 0, 0},
		{fmt.Errorf("original error"), 0, 1 * time.Hour},
		{fmt.Errorf("original error"), 1, 2 * time.Hour},
		{fmt.Errorf("original error"), 2, 3 * time.Hour},
		{fmt.Errorf("original error"), 3, 0},
	} {
		t.Run(fmt.Sprintf("%d", i), func(t *testing.T) {
			originalError = tt.originalError
			job := &pgxjob.Job{ErrorCount: tt.errorCount}
			earliestRetryTime := time.Now().Add(tt.retryDelay)
			err := fn(context.Background(), job)
			latestRetryTime := time.Now().Add(tt.retryDelay)
			if originalError == nil {
				require.NoError(t, err)
			} else {
				var errorWithRetry *pgxjob.ErrorWithRetry
				if tt.retryDelay == 0 {
					require.False(t, errors.As(err, &errorWithRetry))
				} else {
					require.EqualError(t, err, "original error")
					require.ErrorAs(t, err, &errorWithRetry)
					require.Truef(t, errorWithRetry.RetryAt.After(earliestRetryTime), "RetryAt: %v, earliestRetryTime: %v", errorWithRetry.RetryAt, earliestRetryTime)
					require.Truef(t, errorWithRetry.RetryAt.Before(latestRetryTime), "RetryAt: %v, latestRetryTime: %v", errorWithRetry.RetryAt, latestRetryTime)
				}
			}
		})
	}
}

func BenchmarkRunBackloggedJobs(b *testing.B) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	conn := mustConnect(b)
	mustCleanDatabase(b, conn)
	dbpool := mustNewDBPool(b)

	scheduler, err := pgxjob.NewScheduler(ctx, pgxjob.GetConnFromPoolFunc(dbpool))
	require.NoError(b, err)

	runJobChan := make(chan struct{}, 100)
	err = scheduler.RegisterJobType(ctx, pgxjob.RegisterJobTypeParams{
		Name: "test",
		RunJob: func(ctx context.Context, job *pgxjob.Job) error {
			runJobChan <- struct{}{}
			return nil
		},
	})
	require.NoError(b, err)

	for i := 0; i < b.N; i++ {
		err = scheduler.ScheduleNow(ctx, conn, "test", nil)
		require.NoError(b, err)
	}

	worker, err := scheduler.NewWorker(ctx, pgxjob.WorkerConfig{
		HandleWorkerError: func(worker *pgxjob.Worker, err error) {
			b.Errorf("worker error: %v", err)
		},
	})
	require.NoError(b, err)
	defer worker.Shutdown(context.Background())

	b.ResetTimer()

	startErrChan := make(chan error, 1)
	go func() {
		err := worker.Start()
		startErrChan <- err
	}()

	for i := 0; i < b.N; i++ {
		select {
		case <-runJobChan:
		case err := <-startErrChan:
			b.Fatalf("startErrChan: %v", err)
		case <-ctx.Done():
			b.Fatalf("timed out waiting for jobs to finish: %d", i)
		}
	}

	err = worker.Shutdown(context.Background())
	require.NoError(b, err)

	err = <-startErrChan
	require.NoError(b, err)
}

func benchmarkPostgreSQLParamsInsert(b *testing.B, params_type string) {
	ctx := context.Background()
	conn := mustConnect(b)

	_, err := conn.Exec(ctx, fmt.Sprintf(`create temporary table benchmark_params (
	id bigint primary key generated by default as identity,
	params %s
)`, params_type))
	require.NoError(b, err)

	params := []byte(`{"id":"1234567890","foo":"bar","baz":"quz"}`)

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, err := conn.Exec(ctx, "insert into benchmark_params (params) values ($1)", params)
		require.NoError(b, err)
	}
}

func BenchmarkPostgreSQLParamsInsertJSON(b *testing.B) {
	benchmarkPostgreSQLParamsInsert(b, "json")
}

func BenchmarkPostgreSQLParamsInsertJSONB(b *testing.B) {
	benchmarkPostgreSQLParamsInsert(b, "jsonb")
}

func BenchmarkPostgreSQLParamsInsertText(b *testing.B) {
	benchmarkPostgreSQLParamsInsert(b, "text")
}

func benchmarkPostgreSQLParamsSelect(b *testing.B, params_type string) {
	ctx := context.Background()
	conn := mustConnect(b)

	_, err := conn.Exec(ctx, fmt.Sprintf(`create temporary table benchmark_params (
	id bigint primary key generated by default as identity,
	params %s
)`, params_type))
	require.NoError(b, err)

	params := []byte(`{"id":"1234567890","foo":"bar","baz":"quz"}`)
	_, err = conn.Exec(ctx, "insert into benchmark_params (params) values ($1)", params)
	require.NoError(b, err)

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, err := conn.Exec(ctx, "select * from benchmark_params")
		require.NoError(b, err)
	}
}

func BenchmarkPostgreSQLParamsSelectJSON(b *testing.B) {
	benchmarkPostgreSQLParamsSelect(b, "json")
}

func BenchmarkPostgreSQLParamsSelectJSONB(b *testing.B) {
	benchmarkPostgreSQLParamsSelect(b, "jsonb")
}

func BenchmarkPostgreSQLParamsSelectText(b *testing.B) {
	benchmarkPostgreSQLParamsSelect(b, "text")
}

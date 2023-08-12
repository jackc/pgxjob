package pgxjob_test

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgxjob"
	"github.com/jackc/pgxlisten"
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
	scheduler, err := pgxjob.NewScheduler(ctx, pgxjob.SchedulerConfig{
		AcquireConn: pgxjob.AcquireConnFuncFromPool(dbpool),
		JobTypes: []pgxjob.JobTypeConfig{
			{
				Name: "test",
				RunJob: func(ctx context.Context, job *pgxjob.Job) error {
					jobRanChan <- struct{}{}
					return nil
				},
			},
		},
		HandleError: func(err error) {
			t.Errorf("scheduler HandleError: %v", err)
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

	worker, err := scheduler.StartWorker(ctx, pgxjob.WorkerConfig{})
	require.NoError(t, err)

	select {
	case <-jobRanChan:
	case <-time.After(30 * time.Second):
		t.Fatal("timed out waiting for job to run")
	}

	worker.Shutdown(context.Background())

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
	scheduler, err := pgxjob.NewScheduler(ctx, pgxjob.SchedulerConfig{
		AcquireConn: pgxjob.AcquireConnFuncFromPool(dbpool),
		JobTypes: []pgxjob.JobTypeConfig{
			{
				Name: "test",
				RunJob: func(ctx context.Context, job *pgxjob.Job) error {
					jobRanChan <- struct{}{}
					return nil
				},
			},
		},
		HandleError: func(err error) {
			t.Errorf("scheduler HandleError: %v", err)
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

	worker, err := scheduler.StartWorker(ctx, pgxjob.WorkerConfig{
		PollInterval: 50 * time.Millisecond,
	})
	require.NoError(t, err)

	select {
	case <-jobRanChan:
	case <-time.After(30 * time.Second):
		t.Fatal("timed out waiting for job to run")
	}

	worker.Shutdown(context.Background())

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
	scheduler, err := pgxjob.NewScheduler(ctx, pgxjob.SchedulerConfig{
		AcquireConn: pgxjob.AcquireConnFuncFromPool(dbpool),
		JobTypes: []pgxjob.JobTypeConfig{
			{
				Name: "test",
				RunJob: func(ctx context.Context, job *pgxjob.Job) error {
					jobRanChan <- struct{}{}
					return nil
				},
			},
		},
		HandleError: func(err error) {
			t.Errorf("scheduler HandleError: %v", err)
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

	worker, err := scheduler.StartWorker(ctx, pgxjob.WorkerConfig{
		PollInterval: 50 * time.Millisecond,
	})
	require.NoError(t, err)

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
	scheduler, err := pgxjob.NewScheduler(ctx, pgxjob.SchedulerConfig{
		AcquireConn: pgxjob.AcquireConnFuncFromPool(dbpool),
		JobTypes: []pgxjob.JobTypeConfig{
			{
				Name: "test",
				RunJob: func(ctx context.Context, job *pgxjob.Job) error {
					jobRanChan <- struct{}{}
					return fmt.Errorf("test error")
				},
			},
		},
		HandleError: func(err error) {
			t.Errorf("scheduler HandleError: %v", err)
		},
	})
	require.NoError(t, err)

	err = scheduler.ScheduleNow(ctx, conn, "test", nil)
	require.NoError(t, err)

	job, err := pgxutil.SelectRow(ctx, conn, `select * from pgxjob_asap_jobs`, nil, pgx.RowToStructByPos[asapJob])
	require.NoError(t, err)

	worker, err := scheduler.StartWorker(ctx, pgxjob.WorkerConfig{})
	require.NoError(t, err)

	select {
	case <-jobRanChan:
	case <-time.After(30 * time.Second):
		t.Fatal("timed out waiting for job to run")
	}

	worker.Shutdown(context.Background())

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

	scheduler, err := pgxjob.NewScheduler(ctx, pgxjob.SchedulerConfig{
		AcquireConn: pgxjob.AcquireConnFuncFromPool(dbpool),
		JobTypes: []pgxjob.JobTypeConfig{
			{
				Name: "test",
				RunJob: func(ctx context.Context, job *pgxjob.Job) error {
					return nil
				},
			},
		},
		HandleError: func(err error) {
			t.Errorf("scheduler HandleError: %v", err)
		},
	})
	require.NoError(t, err)

	worker, err := scheduler.StartWorker(ctx, pgxjob.WorkerConfig{})
	require.NoError(t, err)

	err = pgxutil.InsertRow(ctx, conn, "pgxjob_asap_jobs", map[string]any{
		"group_id": 1,  // 1 should always be the default group
		"type_id":  -1, // -1 should never exist
	})
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		n, err := pgxutil.SelectRow(ctx, conn, `select count(*) from pgxjob_asap_jobs`, nil, pgx.RowTo[int32])
		require.NoError(t, err)
		return n == 0
	}, 5*time.Second, 100*time.Millisecond)

	worker.Shutdown(context.Background())

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
	retryAt := time.Now().Add(1 * time.Hour)
	scheduler, err := pgxjob.NewScheduler(ctx, pgxjob.SchedulerConfig{
		AcquireConn: pgxjob.AcquireConnFuncFromPool(dbpool),
		JobTypes: []pgxjob.JobTypeConfig{
			{
				Name: "test",
				RunJob: func(ctx context.Context, job *pgxjob.Job) error {
					jobRanChan <- struct{}{}
					return &pgxjob.ErrorWithRetry{Err: fmt.Errorf("test error"), RetryAt: retryAt}
				},
			},
		},
		HandleError: func(err error) {
			t.Errorf("scheduler HandleError: %v", err)
		},
	})
	require.NoError(t, err)

	err = scheduler.ScheduleNow(ctx, conn, "test", nil)
	require.NoError(t, err)

	worker, err := scheduler.StartWorker(ctx, pgxjob.WorkerConfig{})
	require.NoError(t, err)

	select {
	case <-jobRanChan:
	case <-time.After(30 * time.Second):
		t.Fatal("timed out waiting for job to run")
	}

	worker.Shutdown(context.Background())

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
	scheduler, err := pgxjob.NewScheduler(ctx, pgxjob.SchedulerConfig{
		AcquireConn: pgxjob.AcquireConnFuncFromPool(dbpool),
		JobTypes: []pgxjob.JobTypeConfig{
			{
				Name: "test",
				RunJob: func(ctx context.Context, job *pgxjob.Job) error {
					jobRanChan <- struct{}{}
					return nil
				},
			},
		},
		HandleError: func(err error) {
			t.Errorf("scheduler HandleError: %v", err)
		},
	})
	require.NoError(t, err)

	backlogCount := 10000
	for i := 0; i < backlogCount; i++ {
		err = scheduler.ScheduleNow(ctx, conn, "test", nil)
		require.NoError(t, err)
	}

	worker, err := scheduler.StartWorker(ctx, pgxjob.WorkerConfig{})
	require.NoError(t, err)

	for i := 0; i < backlogCount; i++ {
		select {
		case <-jobRanChan:
		case <-ctx.Done():
			t.Fatal("timed out waiting for job to run")
		}
	}

	worker.Shutdown(context.Background())

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

	scheduler, err := pgxjob.NewScheduler(ctx, pgxjob.SchedulerConfig{
		AcquireConn: pgxjob.AcquireConnFuncFromPool(dbpool),
		JobGroups:   []string{"other"},
		JobTypes: []pgxjob.JobTypeConfig{
			{
				Name: "test",
				RunJob: func(ctx context.Context, job *pgxjob.Job) error {
					return nil
				},
			},
		},
		HandleError: func(err error) {
			t.Errorf("scheduler HandleError: %v", err)
		},
	})
	require.NoError(t, err)

	err = scheduler.ScheduleNow(ctx, conn, "test", nil)
	require.NoError(t, err)

	err = scheduler.Schedule(ctx, conn, "test", nil, pgxjob.JobSchedule{GroupName: "other"})
	require.NoError(t, err)

	worker, err := scheduler.StartWorker(ctx, pgxjob.WorkerConfig{
		PollInterval: 500 * time.Millisecond,
	})
	require.NoError(t, err)

	time.Sleep(5 * time.Second)

	worker.Shutdown(context.Background())

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
	scheduler, err := pgxjob.NewScheduler(ctx, pgxjob.SchedulerConfig{
		AcquireConn: pgxjob.AcquireConnFuncFromPool(dbpool),
		JobTypes: []pgxjob.JobTypeConfig{
			{
				Name: "test",
				RunJob: func(ctx context.Context, job *pgxjob.Job) error {
					select {
					case jobRanChan <- struct{}{}:
					case <-time.After(10 * time.Millisecond):
					}
					return nil
				},
			},
		},
		HandleError: func(err error) {
			t.Errorf("scheduler HandleError: %v", err)
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

	worker, err := scheduler.StartWorker(ctx, pgxjob.WorkerConfig{
		MaxConcurrentJobs: 1,
		MaxPrefetchedJobs: 1000,
	})
	require.NoError(t, err)

	// Wait for at least 10 jobs to have started/run.
	for i := 0; i < 10; i++ {
		select {
		case <-jobRanChan:
		case <-ctx.Done():
			t.Fatal("timed out waiting for job to run")
		}
	}

	worker.Shutdown(context.Background())

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

	scheduler, err := pgxjob.NewScheduler(ctx, pgxjob.SchedulerConfig{
		AcquireConn: pgxjob.AcquireConnFuncFromPool(dbpool),
		JobTypes: []pgxjob.JobTypeConfig{
			{
				Name: "test",
				RunJob: func(ctx context.Context, job *pgxjob.Job) error {
					return nil
				},
			},
		},
		HandleError: func(err error) {
			t.Errorf("scheduler HandleError: %v", err)
		},
	})
	require.NoError(t, err)

	workerConfig := pgxjob.WorkerConfig{}
	workerConfig.SetMinHeartbeatDelayForTest(50 * time.Millisecond)
	workerConfig.SetHeartbeatDelayJitterForTest(50 * time.Millisecond)

	worker, err := scheduler.StartWorker(ctx, workerConfig)
	require.NoError(t, err)

	firstHeartbeat, err := pgxutil.SelectRow(ctx, conn, `select heartbeat from pgxjob_workers where id = $1`, []any{worker.ID}, pgx.RowTo[time.Time])
	require.NoError(t, err)

	require.EventuallyWithT(t, func(c *assert.CollectT) {
		heartbeat, err := pgxutil.SelectRow(ctx, conn, `select heartbeat from pgxjob_workers where id = $1`, []any{worker.ID}, pgx.RowTo[time.Time])
		assert.NoError(c, err)
		assert.True(c, heartbeat.After(firstHeartbeat))
	}, 5*time.Second, 100*time.Millisecond)

	worker.Shutdown(context.Background())
}

func TestWorkerHeartbeatCleansUpDeadWorkers(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	conn := mustConnect(t)
	mustCleanDatabase(t, conn)
	dbpool := mustNewDBPool(t)

	scheduler, err := pgxjob.NewScheduler(ctx, pgxjob.SchedulerConfig{
		AcquireConn: pgxjob.AcquireConnFuncFromPool(dbpool),
		JobTypes: []pgxjob.JobTypeConfig{
			{
				Name: "test",
				RunJob: func(ctx context.Context, job *pgxjob.Job) error {
					return nil
				},
			},
		},
		HandleError: func(err error) {
			t.Errorf("scheduler HandleError: %v", err)
		},
	})
	require.NoError(t, err)

	err = scheduler.ScheduleNow(ctx, conn, "test", nil)
	require.NoError(t, err)

	err = scheduler.Schedule(ctx, conn, "test", nil, pgxjob.JobSchedule{RunAt: time.Now().Add(-30 * time.Minute)})
	require.NoError(t, err)

	groupID, err := pgxutil.SelectRow(ctx, conn, `select id from pgxjob_groups limit 1`, nil, pgx.RowTo[int32])
	require.NoError(t, err)

	deadWorkerID, err := pgxutil.InsertRowReturning(ctx, conn,
		"pgxjob_workers",
		map[string]any{"heartbeat": time.Now().Add(-time.Hour), "group_id": groupID},
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

	workerConfig := pgxjob.WorkerConfig{
		PollInterval: 500 * time.Millisecond,
	}
	workerConfig.SetMinHeartbeatDelayForTest(50 * time.Millisecond)
	workerConfig.SetHeartbeatDelayJitterForTest(50 * time.Millisecond)

	worker, err := scheduler.StartWorker(ctx, workerConfig)
	require.NoError(t, err)

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
}

func TestWorkerShouldLogJobRun(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	conn := mustConnect(t)
	mustCleanDatabase(t, conn)
	dbpool := mustNewDBPool(t)

	scheduler, err := pgxjob.NewScheduler(ctx, pgxjob.SchedulerConfig{
		AcquireConn: pgxjob.AcquireConnFuncFromPool(dbpool),
		JobTypes: []pgxjob.JobTypeConfig{
			{
				Name: "test",
				RunJob: func(ctx context.Context, job *pgxjob.Job) error {
					if job.ErrorCount == 0 {
						return &pgxjob.ErrorWithRetry{Err: fmt.Errorf("failed first time"), RetryAt: time.Now().Add(100 * time.Millisecond)}
					}
					return nil
				},
			},
		},
		HandleError: func(err error) {
			t.Errorf("scheduler HandleError: %v", err)
		},
	})
	require.NoError(t, err)

	err = scheduler.ScheduleNow(ctx, conn, "test", nil)
	require.NoError(t, err)

	worker, err := scheduler.StartWorker(ctx, pgxjob.WorkerConfig{
		PollInterval:    50 * time.Millisecond,
		ShouldLogJobRun: pgxjob.LogFinalJobRuns,
	})
	require.NoError(t, err)

	require.EventuallyWithT(t, func(c *assert.CollectT) {
		jobsStillPending, err := pgxutil.SelectRow(ctx, conn,
			`select (select count(*) from pgxjob_asap_jobs) + (select count(*) from pgxjob_run_at_jobs)`,
			nil,
			pgx.RowTo[int32],
		)
		assert.NoError(c, err)
		assert.EqualValues(c, 0, jobsStillPending)
	}, 30*time.Second, 100*time.Millisecond)

	worker.Shutdown(context.Background())

	jobRunsCount, err := pgxutil.SelectRow(ctx, conn, `select count(*) from pgxjob_job_runs`, nil, pgx.RowTo[int32])
	require.NoError(t, err)
	require.EqualValuesf(t, 1, jobRunsCount, "only one job run should have been logged")

	jobRun, err := pgxutil.SelectRow(ctx, conn, `select * from pgxjob_job_runs`, nil, pgx.RowToStructByPos[jobRun])
	require.NoError(t, err)
	require.False(t, jobRun.LastError.Valid)
}

// This test is actually a benchmark of how much writes the database actually performs. It is used to measure and
// minimize the number of writes.
func TestBenchmarkDatabaseWrites(t *testing.T) {
	if os.Getenv("BENCHMARK_DATABASE_WRITES") == "" {
		t.Skip("skipping benchmark")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	conn := mustConnect(t)
	mustCleanDatabase(t, conn)
	dbpool := mustNewDBPool(t)

	type pgStatWAL struct {
		WALRecords int64
		WALBytes   int64
		WALWrite   int64
	}

	type pgStatTable struct {
		NTupIns  int64
		NTupUpd  int64
		NTupDel  int64
		NDeadTup int64
	}

	// Start with a clean database and stats.
	_, err := conn.Exec(ctx, `vacuum full analyze`)
	require.NoError(t, err)

	startStatWAL, err := pgxutil.SelectRow(ctx, conn, `select wal_records, wal_bytes, wal_write from pg_stat_wal`, nil, pgx.RowToStructByPos[pgStatWAL])
	require.NoError(t, err)

	startStatTable, err := pgxutil.SelectRow(ctx, conn, `select n_tup_ins, n_tup_upd, n_tup_del, n_dead_tup from pg_stat_all_tables where relname = 'pgxjob_asap_jobs'`, nil, pgx.RowToStructByPos[pgStatTable])
	require.NoError(t, err)

	jobRanChan := make(chan struct{}, 100)
	scheduler, err := pgxjob.NewScheduler(ctx, pgxjob.SchedulerConfig{
		AcquireConn: pgxjob.AcquireConnFuncFromPool(dbpool),
		JobTypes: []pgxjob.JobTypeConfig{
			{
				Name: "test",
				RunJob: func(ctx context.Context, job *pgxjob.Job) error {
					jobRanChan <- struct{}{}
					return nil
				},
			},
		},
		HandleError: func(err error) {
			t.Errorf("scheduler HandleError: %v", err)
		},
	})
	require.NoError(t, err)

	totalJobs := 0

	worker, err := scheduler.StartWorker(ctx, pgxjob.WorkerConfig{
		ShouldLogJobRun: func(worker *pgxjob.Worker, job *pgxjob.Job, startTime, endTime time.Time, err error) bool {
			return false
		},
	})
	require.NoError(t, err)

	listener := &pgxlisten.Listener{
		Connect: func(ctx context.Context) (*pgx.Conn, error) {
			return pgx.Connect(ctx, fmt.Sprintf("dbname=%s", os.Getenv("PGXJOB_TEST_DATABASE")))
		},
	}

	listener.Handle(pgxjob.PGNotifyChannel, worker)

	listenerCtx, listenerCtxCancel := context.WithCancel(ctx)
	defer listenerCtxCancel()
	listenErrChan := make(chan error)
	go func() {
		err := listener.Listen(listenerCtx)
		if err != nil && !errors.Is(err, context.Canceled) {
			listenErrChan <- err
		}
		close(listenErrChan)
	}()

	for i := 0; i < 100_000; i++ {
		err = scheduler.ScheduleNow(ctx, conn, "test", nil)
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

	pendingASAPJobsCount, err := pgxutil.SelectRow(ctx, conn, `select count(*) from pgxjob_asap_jobs`, nil, pgx.RowTo[int32])
	require.NoError(t, err)
	require.EqualValues(t, 0, pendingASAPJobsCount)

	pendingRunAtJobsCount, err := pgxutil.SelectRow(ctx, conn, `select count(*) from pgxjob_run_at_jobs`, nil, pgx.RowTo[int32])
	require.NoError(t, err)
	require.EqualValues(t, 0, pendingRunAtJobsCount)

	jobRunsCount, err := pgxutil.SelectRow(ctx, conn, `select count(*) from pgxjob_job_runs`, nil, pgx.RowTo[int32])
	require.NoError(t, err)
	require.EqualValues(t, 0, jobRunsCount)

	listenerCtxCancel()
	err = <-listenErrChan
	require.NoError(t, err)

	endStatWAL, err := pgxutil.SelectRow(ctx, conn, `select wal_records, wal_bytes, wal_write from pg_stat_wal`, nil, pgx.RowToStructByPos[pgStatWAL])
	require.NoError(t, err)

	endStatTable, err := pgxutil.SelectRow(ctx, conn, `select n_tup_ins, n_tup_upd, n_tup_del, n_dead_tup from pg_stat_all_tables where relname = 'pgxjob_asap_jobs'`, nil, pgx.RowToStructByPos[pgStatTable])
	require.NoError(t, err)

	t.Logf("wal_records: %d", endStatWAL.WALRecords-startStatWAL.WALRecords)
	t.Logf("wal_bytes: %d", endStatWAL.WALBytes-startStatWAL.WALBytes)
	t.Logf("wal_write: %d", endStatWAL.WALWrite-startStatWAL.WALWrite)
	t.Logf("n_tup_ins: %d", endStatTable.NTupIns-startStatTable.NTupIns)
	t.Logf("n_tup_upd: %d", endStatTable.NTupUpd-startStatTable.NTupUpd)
	t.Logf("n_tup_del: %d", endStatTable.NTupDel-startStatTable.NTupDel)
	t.Logf("n_dead_tup: %d", endStatTable.NDeadTup-startStatTable.NDeadTup)
}

// TestStress creates multiple workers and schedules jobs while they are being worked.
func TestStress(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	conn := mustConnect(t)
	mustCleanDatabase(t, conn)
	dbpool := mustNewDBPool(t)

	t1JobsQueued := 0
	t1JobsRan := 0
	t1JobRanChan := make(chan struct{})

	t2JobsQueued := 0
	t2JobsRan := 0
	t2JobRanChan := make(chan struct{})

	scheduler, err := pgxjob.NewScheduler(ctx, pgxjob.SchedulerConfig{
		AcquireConn: pgxjob.AcquireConnFuncFromPool(dbpool),
		JobGroups:   []string{"other"},
		JobTypes: []pgxjob.JobTypeConfig{
			{
				Name: "t1",
				RunJob: pgxjob.RetryLinearBackoff(func(ctx context.Context, job *pgxjob.Job) error {
					if rand.Intn(100) == 0 {
						return errors.New("random error")
					}
					select {
					case t1JobRanChan <- struct{}{}:
					case <-time.NewTimer(100 * time.Millisecond).C:
						return errors.New("t1JobRanChan full")
					}
					return nil
				},
					1000, 10*time.Millisecond),
			},
			{
				Name: "t2",
				RunJob: pgxjob.RetryLinearBackoff(func(ctx context.Context, job *pgxjob.Job) error {
					select {
					case t2JobRanChan <- struct{}{}:
					case <-time.NewTimer(100 * time.Millisecond).C:
						return errors.New("t2JobRanChan full")
					}
					return nil
				},
					1000, 10*time.Millisecond),
			},
		},
		HandleError: func(err error) {
			t.Errorf("scheduler HandleError: %v", err)
		},
	})
	require.NoError(t, err)

	w1, err := scheduler.StartWorker(ctx, pgxjob.WorkerConfig{})
	require.NoError(t, err)

	w2, err := scheduler.StartWorker(ctx, pgxjob.WorkerConfig{
		MaxConcurrentJobs: 5,
		MaxPrefetchedJobs: 10,
	})
	require.NoError(t, err)

	w3, err := scheduler.StartWorker(ctx, pgxjob.WorkerConfig{
		GroupName: "other",
	})
	require.NoError(t, err)

	// Schedule a bunch of random jobs.
	for i := 0; i < 100_000; i++ {
		n := rand.Intn(100)
		if n < 5 {
			scheduler.Schedule(ctx, conn, "t1", nil, pgxjob.JobSchedule{RunAt: time.Now().Add(time.Duration(rand.Intn(1000)) * time.Millisecond)})
			t1JobsQueued++
		} else if n < 10 {
			scheduler.Schedule(ctx, conn, "t1", nil, pgxjob.JobSchedule{GroupName: "other", RunAt: time.Now().Add(time.Duration(rand.Intn(1000)) * time.Millisecond)})
			t1JobsQueued++
		} else if n < 15 {
			scheduler.Schedule(ctx, conn, "t2", nil, pgxjob.JobSchedule{GroupName: "other"})
			t1JobsQueued++
		} else if n < 50 {
			scheduler.ScheduleNow(ctx, conn, "t1", nil)
			t1JobsQueued++
		} else {
			scheduler.ScheduleNow(ctx, conn, "t2", nil)
			t2JobsQueued++
		}
	}

	for t1JobsRan+t2JobsRan < (t1JobsQueued+t2JobsQueued)/2 {
		select {
		case <-t1JobRanChan:
			t1JobsRan++
		case <-t2JobRanChan:
			t2JobsRan++
		case <-ctx.Done():
			t.Fatalf("timed out waiting for jobs to finish: %d completed", t1JobsRan+t2JobsRan)
		}
	}

	err = w1.Shutdown(ctx)
	require.NoError(t, err)

	err = w2.Shutdown(ctx)
	require.NoError(t, err)

	err = w3.Shutdown(ctx)
	require.NoError(t, err)

	jobsStillPending, err := pgxutil.SelectRow(ctx, conn,
		`select (select count(*) from pgxjob_asap_jobs) + (select count(*) from pgxjob_run_at_jobs)`,
		nil,
		pgx.RowTo[int32],
	)
	require.NoError(t, err)

	lockedJobs, err := pgxutil.SelectRow(ctx, conn,
		`select (select count(*) from pgxjob_asap_jobs where worker_id is not null) + (select count(*) from pgxjob_run_at_jobs where worker_id is not null)`,
		nil,
		pgx.RowTo[int32],
	)
	require.NoError(t, err)
	require.EqualValues(t, 0, lockedJobs)

	successfulJobs, err := pgxutil.SelectRow(ctx, conn, `select count(*) from pgxjob_job_runs where error is null`, nil, pgx.RowTo[int32])
	require.NoError(t, err)
	require.EqualValues(t, (t1JobsQueued + t2JobsQueued), jobsStillPending+successfulJobs)
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

	runJobChan := make(chan struct{}, 100)
	scheduler, err := pgxjob.NewScheduler(ctx, pgxjob.SchedulerConfig{
		AcquireConn: pgxjob.AcquireConnFuncFromPool(dbpool),
		JobTypes: []pgxjob.JobTypeConfig{
			{
				Name: "test",
				RunJob: func(ctx context.Context, job *pgxjob.Job) error {
					runJobChan <- struct{}{}
					return nil
				},
			},
		},
		HandleError: func(err error) {
			b.Errorf("scheduler HandleError: %v", err)
		},
	})
	require.NoError(b, err)

	for i := 0; i < b.N; i++ {
		err = scheduler.ScheduleNow(ctx, conn, "test", nil)
		require.NoError(b, err)
	}

	worker, err := scheduler.StartWorker(ctx, pgxjob.WorkerConfig{})
	require.NoError(b, err)
	defer worker.Shutdown(context.Background())

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		select {
		case <-runJobChan:
		case <-ctx.Done():
			b.Fatalf("timed out waiting for jobs to finish: %d", i)
		}
	}

	err = worker.Shutdown(context.Background())
	require.NoError(b, err)
}

func BenchmarkRunConcurrentlyInsertedJobs(b *testing.B) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	conn := mustConnect(b)
	mustCleanDatabase(b, conn)
	dbpool := mustNewDBPool(b)

	runJobChan := make(chan struct{}, 100)
	scheduler, err := pgxjob.NewScheduler(ctx, pgxjob.SchedulerConfig{
		AcquireConn: pgxjob.AcquireConnFuncFromPool(dbpool),
		JobTypes: []pgxjob.JobTypeConfig{
			{
				Name: "test",
				RunJob: func(ctx context.Context, job *pgxjob.Job) error {
					runJobChan <- struct{}{}
					return nil
				},
			},
		},
		HandleError: func(err error) {
			b.Errorf("scheduler HandleError: %v", err)
		},
	})
	require.NoError(b, err)

	worker, err := scheduler.StartWorker(ctx, pgxjob.WorkerConfig{})
	require.NoError(b, err)
	defer worker.Shutdown(context.Background())

	listener := &pgxlisten.Listener{
		Connect: func(ctx context.Context) (*pgx.Conn, error) {
			return pgx.Connect(ctx, fmt.Sprintf("dbname=%s", os.Getenv("PGXJOB_TEST_DATABASE")))
		},
	}

	listener.Handle(pgxjob.PGNotifyChannel, worker)

	listenerCtx, listenerCtxCancel := context.WithCancel(ctx)
	defer listenerCtxCancel()
	listenErrChan := make(chan error)
	go func() {
		err := listener.Listen(listenerCtx)
		if err != nil && !errors.Is(err, context.Canceled) {
			listenErrChan <- err
		}
		close(listenErrChan)
	}()

	// Wait a little for the listener to start. Otherwise we might miss the first notification which would the first
	// benchmark where b.N == 1 to take PollDuration time.
	time.Sleep(time.Second)

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		err = scheduler.ScheduleNow(ctx, conn, "test", nil)
		require.NoError(b, err)
	}

	for i := 0; i < b.N; i++ {
		select {
		case <-runJobChan:
		case <-ctx.Done():
			b.Fatalf("timed out waiting for jobs to finish: %d", i)
		}
	}

	err = worker.Shutdown(context.Background())
	require.NoError(b, err)

	listenerCtxCancel()
	err = <-listenErrChan
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

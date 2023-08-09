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
	_, err := conn.Exec(ctx, `delete from pgxjob_jobs`)
	require.NoError(t, err)
	_, err = conn.Exec(ctx, `delete from pgxjob_job_runs`)
	require.NoError(t, err)
}

type pgxjobJob struct {
	ID         int64
	QueuedAt   time.Time
	NextRunAt  pgtype.Timestamptz
	RunAt      pgtype.Timestamptz
	QueueID    int32
	TypeID     int32
	ErrorCount pgtype.Int4
	Priority   int16
	LastError  pgtype.Text
	Params     []byte
}

type pgxjobJobRun struct {
	JobID      int64
	QueuedAt   time.Time
	RunAt      time.Time
	StartedAt  time.Time
	FinishedAt time.Time
	RunNumber  int32
	QueueID    int32
	TypeID     int32
	Priority   int16
	Params     []byte
	LastError  pgtype.Text
}

func TestSimpleEndToEnd(t *testing.T) {
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

	job, err := pgxutil.SelectRow(ctx, conn, `select * from pgxjob_jobs`, nil, pgx.RowToStructByPos[pgxjobJob])
	require.NoError(t, err)

	require.True(t, job.QueuedAt.After(startTime))
	require.True(t, job.QueuedAt.Before(afterScheduleNow))
	require.False(t, job.NextRunAt.Valid)
	require.False(t, job.RunAt.Valid)

	defaultQueueID, err := pgxutil.SelectRow(ctx, conn, `select id from pgxjob_queues where name = 'default'`, nil, pgx.RowTo[int32])
	require.NoError(t, err)
	require.Equal(t, defaultQueueID, job.QueueID)

	testJobTypeID, err := pgxutil.SelectRow(ctx, conn, `select id from pgxjob_types where name = 'test'`, nil, pgx.RowTo[int32])
	require.NoError(t, err)
	require.Equal(t, testJobTypeID, job.TypeID)

	require.False(t, job.ErrorCount.Valid)
	require.EqualValues(t, 100, job.Priority)
	require.False(t, job.LastError.Valid)
	require.Equal(t, []byte(nil), job.Params)

	worker, err := scheduler.NewWorker(pgxjob.WorkerConfig{
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

	jobRun, err := pgxutil.SelectRow(ctx, conn, `select * from pgxjob_job_runs where job_id = $1`, []any{job.ID}, pgx.RowToStructByPos[pgxjobJobRun])
	require.NoError(t, err)

	require.Equal(t, job.ID, jobRun.JobID)
	require.True(t, jobRun.QueuedAt.Equal(job.QueuedAt))
	require.True(t, jobRun.RunAt.Equal(jobRun.QueuedAt))
	require.True(t, jobRun.StartedAt.After(startTime))
	require.True(t, jobRun.StartedAt.Before(afterRunNow))
	require.True(t, jobRun.FinishedAt.After(startTime))
	require.True(t, jobRun.FinishedAt.Before(afterRunNow))
	require.EqualValues(t, 1, jobRun.RunNumber)
	require.Equal(t, job.QueueID, jobRun.QueueID)
	require.Equal(t, job.TypeID, jobRun.TypeID)
	require.Equal(t, job.Priority, jobRun.Priority)
	require.Equal(t, job.Params, jobRun.Params)
	require.Equal(t, job.LastError, jobRun.LastError)
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

	job, err := pgxutil.SelectRow(ctx, conn, `select * from pgxjob_jobs`, nil, pgx.RowToStructByPos[pgxjobJob])
	require.NoError(t, err)

	worker, err := scheduler.NewWorker(pgxjob.WorkerConfig{
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

	_, err = pgxutil.SelectRow(ctx, conn, `select * from pgxjob_jobs where id = $1`, []any{job.ID}, pgx.RowToStructByPos[pgxjobJob])
	require.Error(t, err)
	require.ErrorIs(t, err, pgx.ErrNoRows)

	jobRun, err := pgxutil.SelectRow(ctx, conn, `select * from pgxjob_job_runs where job_id = $1`, []any{job.ID}, pgx.RowToStructByPos[pgxjobJobRun])
	require.NoError(t, err)

	require.Equal(t, job.ID, jobRun.JobID)
	require.True(t, jobRun.QueuedAt.Equal(job.QueuedAt))
	require.True(t, jobRun.RunAt.Equal(jobRun.QueuedAt))
	require.True(t, jobRun.StartedAt.After(startTime))
	require.True(t, jobRun.StartedAt.Before(afterRunNow))
	require.True(t, jobRun.FinishedAt.After(startTime))
	require.True(t, jobRun.FinishedAt.Before(afterRunNow))
	require.EqualValues(t, 1, jobRun.RunNumber)
	require.Equal(t, job.QueueID, jobRun.QueueID)
	require.Equal(t, job.TypeID, jobRun.TypeID)
	require.Equal(t, job.Priority, jobRun.Priority)
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

	worker, err := scheduler.NewWorker(pgxjob.WorkerConfig{
		HandleWorkerError: func(worker *pgxjob.Worker, err error) {
			t.Errorf("worker error: %v", err)
		},
	})
	require.NoError(t, err)

	err = pgxutil.InsertRow(ctx, conn, "pgxjob_jobs", map[string]any{
		"queued_at": time.Now(),
		"queue_id":  1,  // 1 should always be the default queue
		"type_id":   -1, // -1 should never exist
		"priority":  0,
	})
	require.NoError(t, err)

	startErrChan := make(chan error)
	go func() {
		err := worker.Start()
		startErrChan <- err
	}()

	require.Eventually(t, func() bool {
		n, err := pgxutil.SelectRow(ctx, conn, `select count(*) from pgxjob_jobs`, nil, pgx.RowTo[int32])
		require.NoError(t, err)
		return n == 0
	}, 5*time.Second, 100*time.Millisecond)

	worker.Shutdown(context.Background())

	err = <-startErrChan
	require.NoError(t, err)

	jobRun, err := pgxutil.SelectRow(ctx, conn, `select * from pgxjob_job_runs`, nil, pgx.RowToStructByPos[pgxjobJobRun])
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

	worker, err := scheduler.NewWorker(pgxjob.WorkerConfig{
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

	job, err := pgxutil.SelectRow(ctx, conn, `select * from pgxjob_jobs`, nil, pgx.RowToStructByPos[pgxjobJob])
	require.NoError(t, err)
	require.EqualValues(t, 1, job.ErrorCount.Int32)
	require.Equal(t, "test error", job.LastError.String)

	jobRun, err := pgxutil.SelectRow(ctx, conn, `select * from pgxjob_job_runs where job_id = $1`, []any{job.ID}, pgx.RowToStructByPos[pgxjobJobRun])
	require.NoError(t, err)

	require.Equal(t, job.ID, jobRun.JobID)
	require.True(t, jobRun.QueuedAt.Equal(job.QueuedAt))
	require.True(t, jobRun.RunAt.Equal(jobRun.QueuedAt))
	require.True(t, jobRun.StartedAt.After(startTime))
	require.True(t, jobRun.StartedAt.Before(afterRunNow))
	require.True(t, jobRun.FinishedAt.After(startTime))
	require.True(t, jobRun.FinishedAt.Before(afterRunNow))
	require.EqualValues(t, 1, jobRun.RunNumber)
	require.Equal(t, job.QueueID, jobRun.QueueID)
	require.Equal(t, job.TypeID, jobRun.TypeID)
	require.Equal(t, job.Priority, jobRun.Priority)
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

	worker, err := scheduler.NewWorker(pgxjob.WorkerConfig{
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

	jobsStillQueued, err := pgxutil.SelectRow(ctx, conn, `select count(*) from pgxjob_jobs`, nil, pgx.RowTo[int32])
	require.NoError(t, err)
	require.EqualValues(t, 0, jobsStillQueued)

	jobsRun, err := pgxutil.SelectRow(ctx, conn, `select count(*) from pgxjob_job_runs`, nil, pgx.RowTo[int32])
	require.NoError(t, err)
	require.EqualValues(t, backlogCount, jobsRun)
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

	worker, err := scheduler.NewWorker(pgxjob.WorkerConfig{
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

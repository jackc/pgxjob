package pgxjob_test

import (
	"context"
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

func TestSchedulerSimpleEndToEnd(t *testing.T) {
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

	workerErrChan := make(chan error)
	worker, err := scheduler.NewWorker(pgxjob.WorkerConfig{
		HandleWorkerError: func(worker *pgxjob.Worker, err error) {
			workerErrChan <- err
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
	case err := <-workerErrChan:
		t.Fatalf("workerErrChan: %v", err)
	case <-time.After(30 * time.Second):
		t.Fatal("timed out waiting for job to run")
	}

	worker.Shutdown(context.Background())

	err = <-startErrChan
	require.NoError(t, err)

	afterRunNow := time.Now()

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

	workerErrChan := make(chan error, 1)
	worker, err := scheduler.NewWorker(pgxjob.WorkerConfig{
		HandleWorkerError: func(worker *pgxjob.Worker, err error) {
			select {
			case workerErrChan <- err:
			default:
			}
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
		case err := <-workerErrChan:
			b.Fatalf("workerErrChan: %v", err)
		case <-ctx.Done():
			b.Fatalf("timed out waiting for jobs to finish: %d", i)
		}
	}

	err = worker.Shutdown(context.Background())
	require.NoError(b, err)

	err = <-startErrChan
	require.NoError(b, err)

	select {
	case err := <-workerErrChan:
		b.Fatalf("workerErrChan: %v", err)
	default:
	}
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

package pgxjob_test

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgxjob"
	"github.com/jackc/pgxutil"
	"github.com/stretchr/testify/require"
)

// mustConnect connects to the database specified by the PGXJOB_TEST_DATABASE environment variable. It automatically
// closes the connection when the test is finished.
func mustConnect(t testing.TB) *pgx.Conn {
	t.Helper()

	config, err := pgx.ParseConfig(os.Getenv("PGXJOB_TEST_DATABASE"))
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

func mustCleanDatabase(t testing.TB, conn *pgx.Conn) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	_, err := conn.Exec(ctx, `delete from pgxjob_jobs`)
	require.NoError(t, err)
}

func TestSchedulerSimpleEndToEnd(t *testing.T) {
	startTime := time.Now()

	conn := mustConnect(t)
	mustCleanDatabase(t, conn)

	scheduler := pgxjob.NewScheduler()
	err := scheduler.RegisterJobType(pgxjob.JobType{
		Name: "test",
		RunJob: func(ctx context.Context, paramsJSON []byte) error {
			return nil
		},
	})
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	err = scheduler.ScheduleNow(ctx, conn, "test", nil)
	require.NoError(t, err)

	afterScheduleNow := time.Now()

	type pgxjobJob struct {
		ID          int64
		QueueName   string
		Priority    int32
		Type        string
		Params      []byte
		QueuedAt    time.Time
		RunAt       time.Time
		LockedUntil pgtype.Timestamptz
		ErrorCount  int32
		LastError   pgtype.Text
	}

	job, err := pgxutil.SelectRow(ctx, conn, `select * from pgxjob_jobs`, nil, pgx.RowToStructByPos[pgxjobJob])
	require.NoError(t, err)

	require.Equal(t, "default", job.QueueName)
	require.EqualValues(t, 100, job.Priority)
	require.Equal(t, "test", job.Type)
	require.Equal(t, []byte(nil), job.Params)
	require.True(t, job.QueuedAt.After(startTime))
	require.True(t, job.QueuedAt.Before(afterScheduleNow))
	require.True(t, job.RunAt.After(startTime))
	require.True(t, job.RunAt.Before(afterScheduleNow))
	require.False(t, job.LockedUntil.Valid)
	require.EqualValues(t, 0, job.ErrorCount)
	require.False(t, job.LastError.Valid)
}

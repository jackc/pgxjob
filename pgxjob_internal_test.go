package pgxjob

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCircularJobQueue(t *testing.T) {
	q := newCircularJobQueue(3)

	require.Equal(t, 0, q.Len())
	require.Equal(t, 3, q.Cap())
	require.True(t, q.IsEmpty())
	require.False(t, q.IsFull())

	// Enqueue and dequeue one at a time for multiple times around the circular queue.
	for i := 0; i < 10; i++ {
		q.Enqueue(&Job{ID: int64(i)})
		require.Equalf(t, 1, q.Len(), "%d", i)
		require.Equalf(t, 3, q.Cap(), "%d", i)
		require.Falsef(t, q.IsEmpty(), "%d", i)
		require.Falsef(t, q.IsFull(), "%d", i)

		job := q.Dequeue()
		require.Equalf(t, int64(i), job.ID, "%d", i)
		require.Equalf(t, 0, q.Len(), "%d", i)
		require.Equalf(t, 3, q.Cap(), "%d", i)
		require.Truef(t, q.IsEmpty(), "%d", i)
		require.Falsef(t, q.IsFull(), "%d", i)
	}

	// Fill the queue.
	q.Enqueue(&Job{ID: 1})
	q.Enqueue(&Job{ID: 2})
	q.Enqueue(&Job{ID: 3})
	require.Equal(t, 3, q.Len())
	require.Equal(t, 3, q.Cap())
	require.False(t, q.IsEmpty())
	require.True(t, q.IsFull())

	job := q.Dequeue()
	require.Equal(t, int64(1), job.ID)
	require.Equal(t, 2, q.Len())
	require.Equal(t, 3, q.Cap())
	require.False(t, q.IsEmpty())
	require.False(t, q.IsFull())

	job = q.Dequeue()
	require.Equal(t, int64(2), job.ID)
	require.Equal(t, 1, q.Len())
	require.Equal(t, 3, q.Cap())
	require.False(t, q.IsEmpty())
	require.False(t, q.IsFull())

	job = q.Dequeue()
	require.Equal(t, int64(3), job.ID)
	require.Equal(t, 0, q.Len())
	require.Equal(t, 3, q.Cap())
	require.True(t, q.IsEmpty())
	require.False(t, q.IsFull())
}

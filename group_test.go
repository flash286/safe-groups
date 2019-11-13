package safe_egroups

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"github.com/stretchr/testify/assert"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestRunner_Run(t *testing.T) {

	t.Run("all_tasks_finished", func(t *testing.T) {
		ctx := context.Background()
		s := ""
		buf := bytes.NewBufferString(s)
		mtx := sync.Mutex{}

		runner := NewRunner(false,
			func(context context.Context) error {
				mtx.Lock()
				_, err := fmt.Fprint(buf, "Task A")
				mtx.Unlock()
				return err
			},
			func(context context.Context) error {
				mtx.Lock()
				_, err := fmt.Fprint(buf, "Task B")
				mtx.Unlock()
				return err
			},
			func(context context.Context) error {
				mtx.Lock()
				_, err := fmt.Fprint(buf, "Task C")
				mtx.Unlock()
				return err
			},
			func(context context.Context) error {
				mtx.Lock()
				_, err := fmt.Fprint(buf, "Task D")
				mtx.Unlock()
				return err
			},
		)

		err := runner.Do(ctx)
		assert.Nil(t, err)

		for _, st := range []string{"A", "B", "C", "D"} {
			assert.Contains(t, buf.String(), fmt.Sprintf("Task %v", st))
		}
	})

	t.Run("stop_if_fail", func(t *testing.T) {
		ctx := context.Background()
		c := int64(0)

		runner := NewRunner(true,
			func(context context.Context) error {
				return errors.New("error")
			},
			func(context context.Context) error {
				time.Sleep(time.Millisecond * 50)
				atomic.AddInt64(&c, 1)
				return nil
			},
			func(context context.Context) error {
				time.Sleep(time.Millisecond * 50)
				atomic.AddInt64(&c, 1)
				return nil
			},
			func(context context.Context) error {
				time.Sleep(time.Millisecond * 50)
				atomic.AddInt64(&c, 1)
				return nil
			},
		)

		err := runner.Do(ctx)
		assert.NotNil(t, err)
		assert.Less(t, c, int64(1))
		assert.Equal(t, "error", err.Error())

	})

	t.Run("don't_stop_if_fails", func(t *testing.T) {
		ctx := context.Background()
		c := int64(0)

		runner := NewRunner(false,
			func(context context.Context) error {
				return errors.New("error 1")
			},
			func(context context.Context) error {
				return errors.New("error 2")
			},
			func(context context.Context) error {
				time.Sleep(time.Millisecond * 50)
				atomic.AddInt64(&c, 2)
				return nil
			},
			func(context context.Context) error {
				return errors.New("error 3")
			},
		).WithWorkerPool(2)

		err := runner.Do(ctx)
		assert.NotNil(t, err)
		assert.Equal(t, c, int64(2))
		assert.Contains(t, err.Error(), "error 1\n")
		assert.Contains(t, err.Error(), "error 2\n")
		assert.Contains(t, err.Error(), "error 3\n")
	})

	t.Run("panic_occurred", func(t *testing.T) {
		ctx := context.Background()
		c := int64(0)
		runner := NewRunner(false,
			func(context context.Context) error {
				panic("i'm panic, catch me!")
			},
			func(context context.Context) error {
				atomic.AddInt64(&c, 1)
				return nil // Doing well
			},
		)

		err := runner.Do(ctx)

		assert.NotNil(t, err)
		assert.Equal(t, c, int64(1))                         // part of job has been done!
		assert.Contains(t, err.Error(), "group_test.go:125") // checking stacktrace
		assert.Contains(t, err.Error(), "worker_pool.go:42") // checking stacktrace
		assert.Contains(t, err.Error(), "worker_pool.go:26") // checking stacktrace
		assert.Contains(t, err.Error(), "i'm panic")         // checking panic body
	})

	t.Run("ctx_with_timeout", func(t *testing.T) {
		timeout := time.Millisecond * 20
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		defer cancel()
		c := int64(0)
		runner := NewRunner(true,
			func(context context.Context) error {
				time.Sleep(time.Millisecond * 75)
				atomic.AddInt64(&c, 1)
				return nil
			},
			func(context context.Context) error {
				time.Sleep(time.Millisecond * 25)
				atomic.AddInt64(&c, 1)
				time.Sleep(time.Millisecond * 50)
				atomic.AddInt64(&c, 1)
				return nil // Doing well
			},
		)
		startTime := time.Now()
		err := runner.Do(ctx)
		duration := int64(time.Now().Sub(startTime).Seconds() * float64(1000)) // Milliseconds
		if assert.NotNil(t, err) {
			assert.Equal(t, TimeoutError, err)
			assert.Less(t, duration, int64(timeout.Seconds()*1000*1.3))
		}
		assert.Equal(t, int64(0), c)
	})

	t.Run("ctx_canceled_before_all_tasks_start", func(t *testing.T) {
		timeout := time.Second * 20
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		c := int64(0)
		runner := NewRunner(true,
			func(context context.Context) error {
				time.Sleep(time.Millisecond * 5)
				atomic.AddInt64(&c, 1)
				return nil
			},
			func(context context.Context) error {
				time.Sleep(time.Millisecond * 5)
				atomic.AddInt64(&c, 1)
				return nil // Doing well
			},
		)
		cancel()
		err := runner.Do(ctx)
		assert.Error(t, err)
		assert.Equal(t, int64(0), c)
		assert.Equal(t, err.Error(), "context canceled")
	})

	t.Run("worker_pool", func(t *testing.T) {
		c := int64(0)
		var mtx sync.Mutex
		maxC := int64(0)

		runner := NewRunner(true,
			func(context context.Context) error {
				defer func() {
					mtx.Lock()
					if c > maxC {
						maxC = c
					}
					c--
					mtx.Unlock()
				}()
				atomic.AddInt64(&c, 1)
				time.Sleep(time.Millisecond * time.Duration(rand.Intn(50)))
				return nil
			},
			func(context context.Context) error {
				defer func() {
					mtx.Lock()
					if c > maxC {
						maxC = c
					}
					c--
					mtx.Unlock()
				}()
				atomic.AddInt64(&c, 1)
				time.Sleep(time.Millisecond * time.Duration(rand.Intn(50)))
				return nil
			},
			func(context context.Context) error {
				defer func() {
					mtx.Lock()
					if c > maxC {
						maxC = c
					}
					c--
					mtx.Unlock()
				}()
				atomic.AddInt64(&c, 1)
				time.Sleep(time.Millisecond * time.Duration(rand.Intn(50)))
				return nil
			},
			func(context context.Context) error {
				defer func() {
					mtx.Lock()
					if c > maxC {
						maxC = c
					}
					c--
					mtx.Unlock()
				}()
				atomic.AddInt64(&c, 1)
				time.Sleep(time.Millisecond * time.Duration(rand.Intn(50)))
				return nil
			},
		).WithWorkerPool(2)
		err := runner.Do(context.Background())
		assert.Nil(t, err)
		assert.Greater(t, int64(3), maxC)
	})
}

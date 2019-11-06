package safe_egroups

import (
	"bytes"
	"context"
	"fmt"
	"runtime"
	"sync"
	"time"
)

var (
	TimeoutError    = fmt.Errorf("timeout error")
	aggregatedError = fmt.Errorf("combined error:\n")
)

type Task func(context.Context) error

type Runner struct {
	Tasks       []Task
	StopIfError bool
	wg          sync.WaitGroup
	PoolSize    int
}

func NewRunner(stopIfFail bool, tasks ...Task) *Runner {
	return &Runner{Tasks: tasks, StopIfError: stopIfFail, PoolSize: len(tasks)}
}

func (r *Runner) WithWorkerPool(size int) *Runner {
	r.PoolSize = size
	return r
}

func (r *Runner) Do(parent context.Context) error {
	tasksCount := len(r.Tasks)

	resultChannel := make(chan error, tasksCount)

	r.wg.Add(len(r.Tasks))

	wp := NewWorkerPool(r.PoolSize, parent, tasksCount)
	wp.Run(resultChannel)
	defer wp.Stop()

	for _, task := range r.Tasks {
		wp.JobsChannel <- task
	}

	var didError bool
	var tasksFinished int

	for tasksFinished < tasksCount {
		select {
		case <-parent.Done():
			if deadline, ok := parent.Deadline(); ok && deadline.Before(time.Now()) {
				return TimeoutError
			}
			return parent.Err()
		case err := <-resultChannel:
			if err != nil {
				if r.StopIfError || len(r.Tasks) == 1 {
					return err
				}
				aggregatedError = fmt.Errorf("%v%v\n", aggregatedError, err)
				didError = true
			}
		}
		tasksFinished++
	}

	if didError {
		return aggregatedError
	}

	return nil
}

// Just write the error to a error channel
// when a goroutine of a task panics
func safePanic(resultChannel chan<- error) {
	if r := recover(); r != nil {
		resultChannel <- wrapPanic(r)
	}
}

// Add miningful message to the error message
// for esiear debugging
func wrapPanic(recovered interface{}) error {
	var buf [16384]byte
	stack := buf[0:runtime.Stack(buf[:], false)]
	return fmt.Errorf("safe_group.Run: panic %v\n%v", recovered, chopStack(stack, "panic("))
}

func chopStack(s []byte, panicText string) string {
	f := []byte(panicText)
	lfFirst := bytes.IndexByte(s, '\n')
	if lfFirst == -1 {
		return string(s)
	}
	stack := s[lfFirst:]
	panicLine := bytes.Index(stack, f)
	if panicLine == -1 {
		return string(s)
	}
	stack = stack[panicLine+1:]
	for i := 0; i < 2; i++ {
		nextLine := bytes.IndexByte(stack, '\n')
		if nextLine == -1 {
			return string(s)
		}
		stack = stack[nextLine+1:]
	}
	return string(s[:lfFirst+1]) + string(stack)
}

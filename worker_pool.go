package safe_egroups

import (
	"context"
)

type WorkerPool struct {
	Size        int
	JobsChannel chan Task
	ctx         context.Context
	cancelFunc  context.CancelFunc
}

func NewWorkerPool(size int, ctx context.Context, capacity int) WorkerPool {
	ctx, cancel := context.WithCancel(ctx)
	return WorkerPool{
		Size:        size,
		JobsChannel: make(chan Task, capacity),
		ctx:         ctx,
		cancelFunc:  cancel,
	}
}

func (wp *WorkerPool) Run(resultChannel chan error) {
	for i := 0; i < wp.Size; i++ {
		go worker(wp.ctx, wp.JobsChannel, resultChannel)
	}
}

func (wp *WorkerPool) Stop() {
	wp.cancelFunc()
}

func worker(ctx context.Context, jobs <-chan Task, resultChannel chan error) int {
	defer safePanic(resultChannel)
	for {
		select {
		case <-ctx.Done():
			return 1
		case task := <-jobs:
			resultChannel <- task(ctx)
		}
	}
}

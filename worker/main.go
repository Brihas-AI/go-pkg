package worker

import (
	"context"
	"sync"

	"github.com/Brihas-AI/go-pkg/utils"
)

type Pool[T any] struct {
	workerCount int
	jobChan     chan T
	consumerFn  func(ctx context.Context, job T) error
	wg          utils.WaitGroup
	errors      []error
	errorsMu    sync.Mutex
}

func New[T any](workerCount int, consumerFn func(context.Context, T) error) *Pool[T] {
	return &Pool[T]{
		workerCount: workerCount,
		jobChan:     make(chan T),
		consumerFn:  consumerFn,
		errors:      make([]error, 0),
	}
}

func (wp *Pool[T]) Start(ctx context.Context) {
	for i := 0; i < wp.workerCount; i++ {
		wp.wg.Add(1)
		go func(workerID int) {
			defer wp.wg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				case job, ok := <-wp.jobChan:
					if !ok {
						return
					}
					if err := wp.consumerFn(ctx, job); err != nil {
						wp.appendError(err)
					}
				}
			}
		}(i)
	}
}

func (wp *Pool[T]) Push(job T) {
	wp.jobChan <- job
}

func (wp *Pool[T]) Close() {
	close(wp.jobChan)
}

func (wp *Pool[T]) Wait() {
	wp.wg.Wait()
}

func (wp *Pool[T]) appendError(err error) {
	wp.errorsMu.Lock()
	defer wp.errorsMu.Unlock()
	wp.errors = append(wp.errors, err)
}

func (wp *Pool[T]) GetErrors() []error {
	wp.errorsMu.Lock()
	defer wp.errorsMu.Unlock()
	return append([]error(nil), wp.errors...) // return a copy
}

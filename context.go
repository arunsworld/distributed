package distributed

import (
	"context"
	"log"
)

type AbortContextMonitoring func()

// returns a context that monitors the given context for cancellations
// additionally returns an abort monitoring function that causes the monitoring to stop from that point onwards
func NewAbortableMonitoredContext(ctx context.Context) (context.Context, AbortContextMonitoring) {
	resultCtx, resultCtxCancel := context.WithCancel(context.Background())
	acm := abortableContextMonitor{
		abortCh:   make(chan struct{}),
		abortedCh: make(chan struct{}),
	}
	go acm.run(ctx, resultCtxCancel)
	return resultCtx, acm.abortMonitoring
}

type abortableContextMonitor struct {
	abortCh   chan struct{}
	abortedCh chan struct{}
}

func (acm abortableContextMonitor) run(ctx context.Context, cancelFunc context.CancelFunc) {
	select {
	case <-ctx.Done():
		cancelFunc()
		close(acm.abortedCh) // just to be on the safe side
	case <-acm.abortCh:
		close(acm.abortedCh)
		return
	}
}

func (acm abortableContextMonitor) abortMonitoring() {
	close(acm.abortCh)
	<-acm.abortedCh
}

func NewWrappedLeaderMonitoredContext(ctx context.Context, leadershipLost <-chan error) context.Context {
	resultCtx, resultCtxCancel := context.WithCancel(context.Background())
	go func() {
		defer resultCtxCancel()
		select {
		case <-ctx.Done():
			return
		case err := <-leadershipLost:
			log.Printf("error: leadership lost: %v", err)
			return
		}
	}()
	return resultCtx
}

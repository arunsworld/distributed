package distributed

import (
	"context"
	"log"
)

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

package provider_test

import (
	"context"
	"testing"
	"time"

	"github.com/arunsworld/distributed/provider"
)

func Test_AbortableContextMonitor(t *testing.T) {
	t.Run("monitors the given context and cancels the returned context when done", func(t *testing.T) {
		// Given
		pCtx, pCtxCancel := context.WithCancel(context.Background())
		ctx, _ := provider.NewAbortableMonitoredContext(pCtx)
		// When: now we cancel the parent context
		pCtxCancel()
		// Then
		<-ctx.Done()
	})
	t.Run("aborts monitoring when asked ignoring the parent context afterwards", func(t *testing.T) {
		// Given
		pCtx, pCtxCancel := context.WithCancel(context.Background())
		ctx, acm := provider.NewAbortableMonitoredContext(pCtx)
		// When: now we abort monitoring the parent context
		acm()
		// and then cancel the parent context
		pCtxCancel()
		// Then
		time.Sleep(time.Millisecond * 10)
		select {
		case <-ctx.Done():
			t.Fatal("monitored context was cancelled after abort...fail")
		default:
		}
	})
}

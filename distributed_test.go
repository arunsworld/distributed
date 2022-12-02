package distributed_test

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/arunsworld/distributed"
)

func Test_AbortableContextMonitor(t *testing.T) {
	t.Run("monitors the given context and cancels the returned context when done", func(t *testing.T) {
		// Given
		pCtx, pCtxCancel := context.WithCancel(context.Background())
		ctx, _ := distributed.NewAbortableMonitoredContext(pCtx)
		// When: now we cancel the parent context
		pCtxCancel()
		// Then
		<-ctx.Done()
	})
	t.Run("aborts monitoring when asked ignoring the parent context afterwards", func(t *testing.T) {
		// Given
		pCtx, pCtxCancel := context.WithCancel(context.Background())
		ctx, acm := distributed.NewAbortableMonitoredContext(pCtx)
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

func Test_NewConcurrency(t *testing.T) {
	t.Run("acquires only one lease across multiple registration requests", func(t *testing.T) {
		// Given
		leaseCreationTrigger := make(chan struct{})
		provider := &testLeaseProvider{leaseCreationTrigger: leaseCreationTrigger}
		c := distributed.NewConcurrency("testApp", "node1", provider)
		// When
		c.RegisterLeadershipRequest("jobA")
		c.RegisterLeadershipRequest("jobB")
		c.RegisterLeadershipRequest("jobC")
		close(leaseCreationTrigger)
		time.Sleep(time.Millisecond) // give it a bit of propagation time
		c.RegisterLeadershipRequest("jobD")
		c.RegisterLeadershipRequest("jobE")
		c.RegisterLeadershipRequest("jobF")
		time.Sleep(time.Millisecond) // give it a bit of propagation time
		// Then
		if provider.calledCount() != 1 {
			t.Fatalf("expected lease acquisition once, got: %d", provider.calledCount())
		}
	})
	t.Run("acquires a new lease on lease loss", func(t *testing.T) {
		// Given
		leaseCreationTrigger := make(chan struct{})
		leaseTimeoutTrigger := make(chan struct{})
		provider := &testLeaseProvider{leaseCreationTrigger: leaseCreationTrigger, leaseTimeoutTrigger: leaseTimeoutTrigger}
		c := distributed.NewConcurrency("testApp", "node1", provider)
		// When
		c.RegisterLeadershipRequest("jobA")
		leaseCreationTrigger <- struct{}{}
		time.Sleep(time.Millisecond) // give it a bit of propagation time
		// now timeout and re-creation
		leaseTimeoutTrigger <- struct{}{}
		leaseCreationTrigger <- struct{}{}
		time.Sleep(time.Millisecond) // give it a bit of propagation time
		// Then
		if provider.calledCount() != 2 {
			t.Fatalf("expected lease acquisition twice, got: %d", provider.calledCount())
		}
	})
	t.Run("triggers leader election and campaign when we have a valid lease", func(t *testing.T) {
		// Given
		leaseCreationTrigger := make(chan struct{})
		provider := &testLeaseProvider{leaseCreationTrigger: leaseCreationTrigger}
		c := distributed.NewConcurrency("testApp", "node1", provider)
		close(leaseCreationTrigger) // whenever lease creation is desired it will succeed
		// When
		leadershipAcquired, _, _ := c.RegisterLeadershipRequest("jobA")
		time.Sleep(time.Millisecond) // give it a bit of propagation time
		// Then
		if len(provider.lease.elections) != 1 {
			t.Fatalf("expected one election, got: %d", len(provider.lease.elections))
		}
		// And
		provider.lease.elections[0].campaignTrigger <- struct{}{}
		<-leadershipAcquired
	})
	t.Run("allows for a leader to resign", func(t *testing.T) {
		// Given
		leaseCreationTrigger := make(chan struct{})
		provider := &testLeaseProvider{leaseCreationTrigger: leaseCreationTrigger}
		c := distributed.NewConcurrency("testApp", "node1", provider)
		close(leaseCreationTrigger) // whenever lease creation is desired it will succeed
		// And
		leadershipAcquired, _, _ := c.RegisterLeadershipRequest("jobA")
		time.Sleep(time.Millisecond) // give it a bit of propagation time
		provider.lease.elections[0].campaignTrigger <- struct{}{}
		<-leadershipAcquired
		// When
		err := c.ResignLeadership(context.Background(), "jobA")
		if err != nil {
			t.Fatal(err)
		}
		if !provider.lease.elections[0].resigned {
			t.Fatal("expected leadership to have resigned, didn't find that to be the case")
		}
	})
	t.Run("close closes the lease", func(t *testing.T) {
		// Given
		leaseCreationTrigger := make(chan struct{})
		provider := &testLeaseProvider{leaseCreationTrigger: leaseCreationTrigger}
		c := distributed.NewConcurrency("testApp", "node1", provider)
		close(leaseCreationTrigger) // whenever lease creation is desired it will succeed
		// And
		// first job - leader
		leadershipAcquired, _, _ := c.RegisterLeadershipRequest("jobA")
		time.Sleep(time.Millisecond) // give it a bit of propagation time
		provider.lease.elections[0].campaignTrigger <- struct{}{}
		<-leadershipAcquired
		// second job - not leader
		c.RegisterLeadershipRequest("jobB")
		time.Sleep(time.Millisecond) // give it a bit of propagation time
		// When
		c.Close()
		if !provider.lease.elections[0].resigned {
			t.Fatal("expected leadership to have resigned, didn't find that to be the case")
		}
		if !provider.lease.leaseClosed {
			t.Fatal("expected lease to be closed, didn't find that to be the case")
		}
	})
	t.Run("leaders are notified of leadership loss when lease is lost (eg. due to network parition)", func(t *testing.T) {
		//Given
		leaseCreationTrigger := make(chan struct{})
		leaseTimeoutTrigger := make(chan struct{})
		provider := &testLeaseProvider{leaseCreationTrigger: leaseCreationTrigger, leaseTimeoutTrigger: leaseTimeoutTrigger}
		c := distributed.NewConcurrency("testApp", "node1", provider)
		close(leaseCreationTrigger) // whenever lease creation is desired it will succeed
		// And
		leadershipAcquired, leadershipLost, _ := c.RegisterLeadershipRequest("jobA")
		time.Sleep(time.Millisecond) // give it a bit of propagation time
		provider.lease.elections[0].campaignTrigger <- struct{}{}
		<-leadershipAcquired
		// When - trigger lease loss
		leaseTimeoutTrigger <- struct{}{}
		// Then
		<-leadershipLost // no further assertion required; because it will hang if there is a bug
	})
}

type testLeaseProvider struct {
	count                int64
	lease                *testLease
	leaseCreationTrigger chan struct{}
	leaseTimeoutTrigger  chan struct{}
}

func (p *testLeaseProvider) AcquireLease(ctx context.Context) (distributed.Lease, error) {
	select {
	case <-ctx.Done():
		return nil, fmt.Errorf("conext cancelled")
	case <-p.leaseCreationTrigger:
		atomic.AddInt64(&p.count, 1)
		p.lease = newTestLease(p.leaseTimeoutTrigger)
		return p.lease, nil
	}
}

func (p *testLeaseProvider) calledCount() int {
	return int(atomic.LoadInt64(&p.count))
}

func newTestLease(leaseTimeoutTrigger chan struct{}) *testLease {
	result := &testLease{
		done: make(chan struct{}),
	}
	go func() {
		<-leaseTimeoutTrigger
		close(result.done)
	}()
	return result
}

type testLease struct {
	done        chan struct{}
	elections   []*election
	leaseClosed bool
}

func (l *testLease) Expired() <-chan struct{} {
	return l.done
}

func (l *testLease) ElectionFor(constituency string) distributed.Election {
	el := &election{
		constituency:    constituency,
		campaignTrigger: make(chan struct{}),
	}
	l.elections = append(l.elections, el)
	return el
}

func (l *testLease) Close() {
	l.leaseClosed = true
}

type election struct {
	count           int64
	constituency    string
	campaignTrigger chan struct{}
	resigned        bool
}

func (e *election) Campaign(ctx context.Context, val string) error {
	select {
	case <-ctx.Done():
		return nil
	case <-e.campaignTrigger:
		atomic.AddInt64(&e.count, 1)
	}
	return nil
}

func (e *election) ReassureLeadership(context.Context, string) error {
	return nil
}

func (e *election) Resign(context.Context) error {
	e.resigned = true
	return nil
}

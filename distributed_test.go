package distributed_test

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/arunsworld/distributed"
	"github.com/arunsworld/distributed/provider"
)

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
	t.Run("on campaign failure, campaign is retried until the lease is valid", func(t *testing.T) {
		//Given
		leaseCreationTrigger := make(chan struct{})
		close(leaseCreationTrigger) // whenever lease creation is desired it will succeed
		ct := make(chan struct{})
		close(ct) // campaign should trigger whenever desired
		provider := &testLeaseProvider{leaseCreationTrigger: leaseCreationTrigger, electionErrorUntil: 1, campaignTrigger: ct}
		c := distributed.NewConcurrency("testApp", "node1", provider)
		// And
		leadershipAcquired, _, _ := c.RegisterLeadershipRequest("jobA")
		time.Sleep(time.Millisecond) // give it a bit of propagation time
		<-leadershipAcquired
	})
	t.Run("obsolete campaign failure should not interfere with new campaign if lease is refreshed meanwhile", func(t *testing.T) {
		//Given we get a lease but have an error filled campaign
		leaseCreationTrigger := make(chan struct{}, 10)
		close(leaseCreationTrigger)
		leaseTimeoutTrigger := make(chan struct{}, 10)
		ct := make(chan struct{}, 10)
		// close(ct) // campaign should trigger whenever desired
		provider := &testLeaseProvider{leaseCreationTrigger: leaseCreationTrigger, leaseTimeoutTrigger: leaseTimeoutTrigger, campaignTrigger: ct, electionErrorUntil: 3}
		c := distributed.NewConcurrency("testApp", "node1", provider)
		// And then lease expires; but subsequent campaign is good
		leadershipAcquired, _, _ := c.RegisterLeadershipRequest("jobA")
		time.Sleep(time.Millisecond) // give it a bit of propagation time
		provider.electionErrorUntil = 0
		ct <- struct{}{}
		leaseTimeoutTrigger <- struct{}{}
		time.Sleep(time.Millisecond) // give it a bit of propagation time
		time.Sleep(time.Second * 2)  // give enough time for campaign retry timeout to have taken effect
		// Then the failed campaign retry does not interfere and we acquire leadership with the right lease
		<-leadershipAcquired
	})
}

type testLeaseProvider struct {
	count                int64
	lease                *testLease
	leaseCreationTrigger chan struct{}
	leaseTimeoutTrigger  chan struct{}
	campaignTrigger      chan struct{}
	electionErrorUntil   int
}

func (p *testLeaseProvider) AcquireLease(ctx context.Context) (provider.Lease, error) {
	select {
	case <-ctx.Done():
		return nil, fmt.Errorf("conext cancelled")
	case <-p.leaseCreationTrigger:
		atomic.AddInt64(&p.count, 1)
		p.lease = newTestLease(p.leaseTimeoutTrigger, p.campaignTrigger, p.electionErrorUntil)
		return p.lease, nil
	}
}

func (p *testLeaseProvider) calledCount() int {
	return int(atomic.LoadInt64(&p.count))
}

func newTestLease(leaseTimeoutTrigger chan struct{}, campaignTrigger chan struct{}, electionErrorUntil int) *testLease {
	result := &testLease{
		done:               make(chan struct{}),
		electionErrorUntil: electionErrorUntil,
		campaignTrigger:    campaignTrigger,
	}
	go func() {
		<-leaseTimeoutTrigger
		close(result.done)
	}()
	return result
}

type testLease struct {
	done               chan struct{}
	elections          []*election
	leaseClosed        bool
	campaignTrigger    chan struct{}
	electionErrorUntil int
	errorCount         int
}

func (l *testLease) Expired() <-chan struct{} {
	return l.done
}

func (l *testLease) ElectionFor(constituency string) provider.Election {
	var electionErr error
	if l.electionErrorUntil > 0 {
		if l.errorCount < l.electionErrorUntil {
			electionErr = fmt.Errorf("simulated error")
			l.errorCount++
		}
	}
	ct := l.campaignTrigger
	if ct == nil {
		ct = make(chan struct{})
	}
	el := &election{
		constituency:    constituency,
		campaignTrigger: ct,
		err:             electionErr,
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
	err             error
}

func (e *election) Campaign(ctx context.Context, val string) error {
	if e.err != nil {
		// time.Sleep(time.Second)
		return e.err
	}
	select {
	case <-ctx.Done():
		return fmt.Errorf("context cancelled")
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

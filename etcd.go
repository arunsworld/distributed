package distributed

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	etcd "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
)

type ETCDOption func(*etcdConcurrencyService)

// appPrefix should uniquely identify the app
// instanceName should uniquely identify the instance for the given app
func NewETCDConcurrencyService(etcdEndPoints []string, appPrefix, instanceName string, options ...ETCDOption) Concurrency {
	result := &etcdConcurrencyService{
		etcdEndPoints:                       etcdEndPoints,
		appPrefix:                           appPrefix,
		instanceName:                        instanceName,
		leadershipAttemptRetryDelayDuration: time.Second * 5,
		leaseTTL:                            10,
		leadershipReassurancePollingDelay:   time.Second * 10,
		leadershipReassuranceTimeout:        time.Second * 5,
	}
	for _, o := range options {
		o(result)
	}
	return result
}

// if we can't connect to etcd, how long do we wait before trying again
// this maynot really ever get exercised and if proven will be removed
// the reason is etcd connectivity relies on grpc which appears to keep retrying forever until context is cancelled
func WithLeadershipAttemptRetryDelayDuration(t time.Duration) ETCDOption {
	return func(e *etcdConcurrencyService) {
		e.leadershipAttemptRetryDelayDuration = t
	}
}

// in seconds how long should ETCD wait until declaring me dead - in case I crash
func WithLeaseTTL(t int) ETCDOption {
	return func(e *etcdConcurrencyService) {
		e.leaseTTL = t
	}
}

// time between polls to ensure we are still the leader
func WithLeadershipReassurancePollingDelay(t time.Duration) ETCDOption {
	return func(e *etcdConcurrencyService) {
		e.leadershipReassurancePollingDelay = t
	}
}

// timeout when connecting to ETCD to assure leadership
func WithLeadershipReassuranceTimeout(t time.Duration) ETCDOption {
	return func(e *etcdConcurrencyService) {
		e.leadershipReassuranceTimeout = t
	}
}

type etcdConcurrencyService struct {
	// mandatory parameters
	etcdEndPoints []string
	appPrefix     string
	instanceName  string
	// optional parameters
	leadershipAttemptRetryDelayDuration time.Duration
	leaseTTL                            int
	leadershipReassurancePollingDelay   time.Duration
	leadershipReassuranceTimeout        time.Duration
	// internal state
	localCtxCancellation          context.CancelFunc
	leaderSince                   time.Time
	leadershipReassuranceFinished chan struct{}
	client                        *etcd.Client
	session                       *concurrency.Session
	election                      *concurrency.Election
}

func (ecs *etcdConcurrencyService) RegisterLeadershipRequest(ctx context.Context) (<-chan struct{}, <-chan error) {
	leadershipAcquired := make(chan struct{})
	leadershipLost := make(chan error, 1)
	localCtx, localCtxCancellation := context.WithCancel(ctx)
	ecs.localCtxCancellation = localCtxCancellation
	go ecs.run(localCtx, leadershipAcquired, leadershipLost)
	return leadershipAcquired, leadershipLost
}

func (ecs *etcdConcurrencyService) run(ctx context.Context, leadershipAcquired chan<- struct{}, leadershipLost chan<- error) {
	// error is only returned if context is cancelled; nothing further needs doing
	if err := ecs.acquireLeadership(ctx, leadershipAcquired); err != nil {
		return
	}
	// leadership reassurance begins until we quit this function
	ecs.leadershipReassuranceFinished = make(chan struct{})
	defer close(ecs.leadershipReassuranceFinished)
	close(leadershipAcquired)
	// we need to then reassure leadership until it's lost or context is cancelled
	for {
		if err := ecs.reassureLeadership(); err != nil {
			ecs.releaseAndResetResources()
			leadershipLost <- err
			close(leadershipLost)
			return
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(ecs.leadershipReassurancePollingDelay):
		}
	}
}

// error means the context was cancelled
func (ecs *etcdConcurrencyService) acquireLeadership(pctx context.Context, leadershipAcquired chan<- struct{}) error {
	// we need a context that monitors the pctx only until we've acquired leadership
	// we monitor the pctx until then to pass it to GRPC and other calls to abort them in the event we need to exit early
	// but once we acquire leadership we don't want to react to pctx to avoid issues during resignation and other such calls
	ctx, abortContextMonitoring := NewAbortableMonitoredContext(pctx)
	// keep attempting leadership until successful
	for {
		if err := ecs.attemptLeadershipAcquisition(ctx); err != nil {
			log.Printf("App: %s. Instance: %s. Leadership acquisition transient error: %v", ecs.appPrefix, ecs.instanceName, err)
			ecs.releaseAndResetResources()
			select {
			case <-time.After(ecs.leadershipAttemptRetryDelayDuration):
			case <-ctx.Done():
				return fmt.Errorf("context cancelled")
			}
		} else {
			abortContextMonitoring() // etcd connection should no longer track ctx
			return nil
		}
	}
}

func (ecs *etcdConcurrencyService) releaseAndResetResources() {
	if ecs.election != nil {
		ecs.election = nil
	}
	if ecs.session != nil {
		ecs.session.Close()
		ecs.session = nil
	}
	if ecs.client != nil {
		ecs.client.Close()
		ecs.client = nil
	}
}

func (ecs *etcdConcurrencyService) attemptLeadershipAcquisition(ctx context.Context) error {
	client, err := etcd.New(etcd.Config{Endpoints: ecs.etcdEndPoints, Context: ctx})
	if err != nil {
		return fmt.Errorf("error creating a new etcd client: %v", err)
	}
	ecs.client = client

	session, err := concurrency.NewSession(client, concurrency.WithContext(ctx), concurrency.WithTTL(ecs.leaseTTL))
	if err != nil {
		return fmt.Errorf("error creating etcd session: %v", err)
	}
	ecs.session = session

	log.Printf("App: %s. Instance: %s. Standing in election.", ecs.appPrefix, ecs.instanceName)
	election := concurrency.NewElection(session, fmt.Sprintf("%s/leader", ecs.appPrefix))
	data := struct {
		Instance               string
		SeekingLeadershipSince time.Time
	}{
		Instance:               ecs.instanceName,
		SeekingLeadershipSince: time.Now(),
	}
	val, err := json.Marshal(data)
	if err != nil {
		return fmt.Errorf("error marshalling value in preparing for election: %v", err)
	}
	if err := election.Campaign(ctx, string(val)); err != nil {
		return fmt.Errorf("error campaigning in election: %v", err)
	}
	ecs.election = election
	ecs.leaderSince = time.Now()

	// we have won the campaign (apparently)
	// since it might have been a long while we should reassure leadership
	if err := ecs.reassureLeadership(); err != nil {
		return err
	}
	return nil
}

func (ecs *etcdConcurrencyService) reassureLeadership() error {
	ctx, cancel := context.WithTimeout(context.Background(), ecs.leadershipReassuranceTimeout)
	defer cancel()

	data := struct {
		Instance    string
		LeaderSince time.Time
		LastSeen    time.Time
	}{
		Instance:    ecs.instanceName,
		LeaderSince: ecs.leaderSince,
		LastSeen:    time.Now(),
	}

	val, err := json.Marshal(data)
	if err != nil {
		return fmt.Errorf("error marshalling proclamation data: %v", err)
	}
	if err := ecs.election.Proclaim(ctx, string(val)); err != nil {
		return fmt.Errorf("error confirming leadership: %v", err)
	}
	return nil
}

func (ecs *etcdConcurrencyService) ResignLeadership(ctx context.Context) error {
	defer ecs.releaseAndResetResources()
	ecs.localCtxCancellation()
	if ecs.election == nil {
		return fmt.Errorf("error resigning: no election registered to resign from")
	}
	<-ecs.leadershipReassuranceFinished // we wait until leadership assurance is finished to avoid race condition with resignation
	if err := ecs.election.Resign(ctx); err != nil {
		return err
	}
	return nil
}

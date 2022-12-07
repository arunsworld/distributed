package provider

import (
	"context"
	"fmt"
	"log"
	"strconv"
	"time"

	etcd "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
)

type ETCDOption func(*etcdLeaseProvider)

// etcd backed LeaseProvider
func NewETCDLeaseProvider(etcdEndPoints []string, options ...ETCDOption) LeaseProvider {
	result := &etcdLeaseProvider{
		etcdEndPoints: etcdEndPoints,
		config:        etcd.Config{Endpoints: etcdEndPoints},
	}
	for _, o := range options {
		o(result)
	}
	return result
}

// in seconds how long should ETCD wait until declaring me dead - in case I crash
func WithLeaseTTL(t int) ETCDOption {
	return func(e *etcdLeaseProvider) {
		e.leaseTTL = t
	}
}

func WithUsernamePassword(username, password string) ETCDOption {
	return func(e *etcdLeaseProvider) {
		e.config.Username = username
		e.config.Password = password
	}
}

func WithETCDConfig(config etcd.Config) ETCDOption {
	return func(e *etcdLeaseProvider) {
		e.config = config
	}
}

type etcdLeaseProvider struct {
	etcdEndPoints []string
	// optional parameters
	leaseTTL int
	// internal
	config       etcd.Config
	currentLease *etcdLease
}

func (p *etcdLeaseProvider) AcquireLease(ctx context.Context) (Lease, error) {
	if p.currentLease != nil {
		p.currentLease.Close()
		p.currentLease = nil
	}

	// we need a context to pass to the session that gets cancelled if ctx is cancelled during session creation
	// to avoid blocking on session creation if an application is trying to stop
	// however, once the session is created, the session context should NOT react to ctx
	// otherwise it will interfere with clean-up operations like Resign and Close
	actx, abortMonitoring := NewAbortableMonitoredContext(ctx)
	var client *etcd.Client
	var session *concurrency.Session
	for {
		var err error
		func() {
			client, err = etcd.New(p.config)
			if err != nil {
				err = fmt.Errorf("error creating a new etcd client: %v", err)
				return
			}
			session, err = concurrency.NewSession(client, concurrency.WithContext(actx), concurrency.WithTTL(p.leaseTTL))
			if err != nil {
				err = fmt.Errorf("error creating a new session: %v", err)
			}
		}()
		// we will get an error if an operation fails (even for context cancellation)
		// therefore, absence of error indicates successful lease acquisition
		if err == nil {
			break
		}
		select {
		case <-ctx.Done():
			return nil, fmt.Errorf("context cancelled")
		default:
		}
		log.Printf("transient error in etcd AcquireLease: %v", err)
		select {
		case <-ctx.Done():
			return nil, fmt.Errorf("context cancelled")
		case <-time.After(time.Second):
		}
	}
	// now that we have a valid session, we no longer need it to reacht to ctx
	abortMonitoring()

	return &etcdLease{
		client:  client,
		session: session,
	}, nil
}

type etcdLease struct {
	client  *etcd.Client
	session *concurrency.Session
}

func (l *etcdLease) ID() string {
	if l.session == nil {
		return ""
	}
	return strconv.FormatInt(int64(l.session.Lease()), 10)
}

func (l *etcdLease) Expired() <-chan struct{} {
	return l.session.Done()
}

func (l *etcdLease) ElectionFor(constituency string) Election {
	election := concurrency.NewElection(l.session, constituency)
	return &etcdElection{
		election:     election,
		constituency: constituency,
	}
}

func (l *etcdLease) Close() {
	if err := l.session.Close(); err != nil {
		log.Printf("warning: error closing session: %v", err)
	}
	if err := l.client.Close(); err != nil {
		log.Printf("warning: error closing client: %v", err)
	}
}

type etcdElection struct {
	election     *concurrency.Election
	constituency string
}

func (e *etcdElection) Campaign(ctx context.Context, val string) error {
	if err := e.election.Campaign(ctx, val); err != nil {
		return fmt.Errorf("error campaigning in %s election: %v", e.constituency, err)
	}
	return nil
}

func (e *etcdElection) ReassureLeadership(ctx context.Context, val string) error {
	if err := e.election.Proclaim(ctx, val); err != nil {
		return fmt.Errorf("error reassuring leadership for %s: %v", e.constituency, err)
	}
	return nil
}

func (e *etcdElection) Resign(ctx context.Context) error {
	if err := e.election.Resign(ctx); err != nil {
		return fmt.Errorf("error resigning leadership for %s: %v", e.constituency, err)
	}
	return nil
}

package provider

import (
	"context"
	"fmt"
	"log"
	"time"

	etcd "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
)

type ETCDOption func(*etcdLeaseProvider)

func NewETCDLeaseProvider(etcdEndPoints []string, options ...ETCDOption) LeaseProvider {
	result := &etcdLeaseProvider{
		etcdEndPoints: etcdEndPoints,
		leaseTTL:      10,
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

type etcdLeaseProvider struct {
	etcdEndPoints []string
	// optional parameters
	leaseTTL int
	// internal
	currentLease *etcdLease
}

func (p *etcdLeaseProvider) AcquireLease(ctx context.Context) (Lease, error) {
	if p.currentLease != nil {
		p.currentLease.Close()
		p.currentLease = nil
	}

	actx, abortMonitoring := NewAbortableMonitoredContext(ctx)
	var client *etcd.Client
	var session *concurrency.Session
	for {
		var err error
		client, err = etcd.New(etcd.Config{Endpoints: p.etcdEndPoints})
		if err != nil {
			return nil, fmt.Errorf("error creating a new etcd client: %v", err)
		}
		session, err = concurrency.NewSession(client, concurrency.WithContext(actx), concurrency.WithTTL(p.leaseTTL))
		if err == nil {
			break
		}
		select {
		case <-ctx.Done():
			return nil, fmt.Errorf("context cancelled")
		default:
		}
		log.Printf("error creating new session while attempting to aquire lease: %v", err)
		select {
		case <-ctx.Done():
			return nil, fmt.Errorf("context cancelled")
		case <-time.After(time.Second):
		}
	}
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
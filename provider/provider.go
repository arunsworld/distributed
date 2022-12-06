package provider

import (
	"context"
	"time"
)

type LeaseProvider interface {
	// if context is cancelled, return an error; otherwise expected to block forever until lease is acquired
	AcquireLease(context.Context) (Lease, error)
}

type Lease interface {
	ID() string
	Expired() <-chan struct{}
	ElectionFor(constituency string) Election
	Close()
}

type Election interface {
	Campaign(context.Context, string) error
	ReassureLeadership(context.Context, string) error
	Resign(context.Context) error
}

type ElectionMetaDataProvider interface {
	// identifying the locking key during election - the lowest denominator of lock
	Constituency(shard string) string
	// value that is used during campaigning for leadership
	CampaignPromise() string
	// value that is used as a part of a heartbeat during leadership
	LeadershipReassurance(electedAt time.Time) string
}

package provider

import (
	"context"
	"time"
)

// A LeaseProvider implementation is backed by a remote endpoint used to implement a distributed lock.
// It facilitates the acquisition of a Lease that indicates liveness of the client as well as the server.
// The LeaseProvider should continuously renew it's lease before expiration via a heartbeat of some sort.
// From a client perspective if the remote endpoint is no longer available or for other reasons indicated by the server,
// an acquired lease should be expired.
// From a server perspective if a lease exprires, other lease holders can acquire locks previously held by the expiring lease holder.
type LeaseProvider interface {
	// if context is cancelled, return an error; otherwise block forever until lease is acquired
	// returning before a lease is acquired and the context still active will be considered a bugs
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

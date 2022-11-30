package distributed

import (
	"context"
)

type Concurrency interface {
	// RegisterLeadershipRequest registers the caller for a leadership election. Returns 2 channels.
	// leadershipAcquired receives a callback when leadership is successfully acquired.
	// blocks forever until leadership is acquired.
	// releases all resources if context is cancelled
	// lostLeadership receives a callback if leadership is subsequently lost after leadership acquisition
	// the included error indicates why the leadership was lost
	RegisterLeadershipRequest(context.Context) (leadershipAcquired <-chan struct{}, leadershipLost <-chan error)
	// A good citizen should always call ResignLeadership before quiting
	// Otherwise the leadership will be locked until TTL during which there will be no leader
	// That's why this is a separate call to give the opportunity to block until resignation is done before quiting application
	ResignLeadership(context.Context) error
}

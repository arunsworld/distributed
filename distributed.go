package distributed

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/arunsworld/distributed/fsm"
	"github.com/arunsworld/distributed/provider"
)

func NewConcurrency(appName, instanceName string, leaseProvider provider.LeaseProvider, maxLeaders int) *Concurrency {
	emdpProvidier := emdpProvidier{appName: appName, instanceName: instanceName}
	return &Concurrency{
		leaseFSM: fsm.NewLeaseFSM(leaseProvider, emdpProvidier, maxLeaders),
	}
}

type Concurrency struct {
	leaseFSM fsm.LeaseFSM
}

// RegisterLeadershipRequest registers the caller for a leadership election. Returns 2 channels.
// leadershipAcquired receives a callback when leadership is successfully acquired.
// blocks forever until leadership is acquired.
// releases all resources if context is cancelled
// lostLeadership receives a callback if leadership is subsequently lost after leadership acquisition
// the included error indicates why the leadership was lost
func (c *Concurrency) RegisterLeadershipRequest(shard string) (<-chan struct{}, <-chan error, error) {
	msg, _resp := fsm.NewLeadershipRegistrationReq(shard)
	c.leaseFSM.Publish(msg)
	resp := <-_resp
	return resp.LeadershipAcquired(), resp.LeadershipLost(), resp.Err()
}

// A good citizen should always call ResignLeadership before quiting
// Otherwise the leadership will be locked until TTL during which there will be no leader
// That's why this is a separate call to give the opportunity to block until resignation is done before quiting application
func (c *Concurrency) ResignLeadership(ctx context.Context, shard string) error {
	msg, resp := fsm.NewLeadershipResignationReq(ctx, shard)
	c.leaseFSM.Publish(msg)
	return <-resp
}

// Close should be called before the Concurrency entity is discarded of to free up any used resources
func (c *Concurrency) Close() {
	msg, resp := fsm.NewLeaseCloseMsg()
	c.leaseFSM.Publish(msg)
	<-resp
}

type emdpProvidier struct {
	appName      string
	instanceName string
}

func (e emdpProvidier) Constituency(shard string) string {
	return fmt.Sprintf("%s/%s/leader", e.appName, shard)
}

func (e emdpProvidier) CampaignPromise() string {
	data := struct {
		Instance               string
		SeekingLeadershipSince time.Time
	}{
		Instance:               e.instanceName,
		SeekingLeadershipSince: time.Now(),
	}
	val, err := json.Marshal(data)
	if err != nil {
		return "{}"
	}
	return string(val)
}

func (e emdpProvidier) LeadershipReassurance(electedAt time.Time) string {
	data := struct {
		Instance    string
		LeaderSince time.Time
	}{
		Instance:    e.instanceName,
		LeaderSince: electedAt,
	}
	val, err := json.Marshal(data)
	if err != nil {
		return "{}"
	}
	return string(val)
}

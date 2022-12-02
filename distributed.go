package distributed

import (
	"context"
	"encoding/json"
	"fmt"
	"time"
)

type LeaseProvider interface {
	// if context is cancelled, return an error; otherwise expected to block forever until lease is acquired
	AcquireLease(context.Context) (Lease, error)
}

type Lease interface {
	Expired() <-chan struct{}
	ElectionFor(constituency string) Election
	Close()
}

type Election interface {
	Campaign(context.Context, string) error
	ReassureLeadership(context.Context, string) error
	Resign(context.Context) error
}

func NewConcurrency(appName, instanceName string, leaseProvider LeaseProvider) *Concurrency {
	emdpProvidier := emdpProvidier{appName: appName, instanceName: instanceName}
	return &Concurrency{
		leaseFSM: newLeaseFSM(leaseProvider, emdpProvidier),
	}
}

type Concurrency struct {
	leaseFSM *leaseFSM
}

// RegisterLeadershipRequest registers the caller for a leadership election. Returns 2 channels.
// leadershipAcquired receives a callback when leadership is successfully acquired.
// blocks forever until leadership is acquired.
// releases all resources if context is cancelled
// lostLeadership receives a callback if leadership is subsequently lost after leadership acquisition
// the included error indicates why the leadership was lost
func (c *Concurrency) RegisterLeadershipRequest(shard string) (<-chan struct{}, <-chan error, error) {
	_resp := make(chan leadershipRegistrationResp)
	c.leaseFSM.mailbox <- leaseFSMMsg{
		msgType: leadershipRegistrationReqMsgType,
		payload: leadershipRegistrationReq{
			shard: shard,
			resp:  _resp,
		},
	}
	resp := <-_resp
	return resp.leadershipAcquired, resp.leadershipLost, resp.err
}

// A good citizen should always call ResignLeadership before quiting
// Otherwise the leadership will be locked until TTL during which there will be no leader
// That's why this is a separate call to give the opportunity to block until resignation is done before quiting application
func (c *Concurrency) ResignLeadership(ctx context.Context, shard string) error {
	resp := make(chan error)
	c.leaseFSM.mailbox <- leaseFSMMsg{
		msgType: leadershipResignationReqMsgType,
		payload: leadershipResignationReq{
			ctx:   ctx,
			shard: shard,
			resp:  resp,
		},
	}
	return <-resp
}

// Close should be called before the Concurrency entity is discarded of to free up any used resources
func (c *Concurrency) Close() {
	resp := make(chan struct{})
	c.leaseFSM.mailbox <- leaseFSMMsg{
		msgType: leaseFSMCloseMsgType,
		payload: resp,
	}
	<-resp
	return
}

type emdpProvidier struct {
	appName      string
	instanceName string
}

func (e emdpProvidier) constituency(shard string) string {
	return fmt.Sprintf("%s/%s/leader", e.appName, shard)
}

func (e emdpProvidier) campaignPromise() string {
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

func (e emdpProvidier) leadershipReassurance(electedAt time.Time) string {
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

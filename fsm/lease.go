package fsm

import (
	"context"

	"github.com/arunsworld/distributed/provider"
)

// LeaseFSM provides a variety of functionality in a concurrent safe way using the concept of a state model and actor model
// It receives leadership registration requests from it's end users upon which it ensures the acquisition of a lease
// and subsequent delegation of leadership election to ElectionFSM.
// It also processes requests for when this election is obtained if a max leadership quota is confirmed.
// It handles resignations by delegating it down and loss of leases.
type LeaseFSM interface {
	Publish(leaseFSMMsg)
}

func NewLeaseFSM(provider provider.LeaseProvider, emdp provider.ElectionMetaDataProvider, maxLeaders int) *leaseFSM {
	result := &leaseFSM{
		provider:   provider,
		emdp:       emdp,
		mailbox:    make(chan leaseFSMMsg, 30),
		maxLeaders: maxLeaders,
	}
	go result.run()
	return result
}

func NewLeadershipRegistrationReq(shard string) (leaseFSMMsg, <-chan LeadershipRegistrationResp) {
	resp := make(chan LeadershipRegistrationResp, 1)
	msg := leaseFSMMsg{
		msgType: leadershipRegistrationReqMsgType,
		leadershipRegistrationReq: leadershipRegistrationReq{
			shard: shard,
			resp:  resp,
		},
	}
	return msg, resp
}

type LeadershipRegistrationResp interface {
	LeadershipAcquired() <-chan struct{}
	LeadershipLost() <-chan error
	Err() error
}

func NewLeaseCreationReq() leaseFSMMsg {
	return leaseFSMMsg{
		msgType: leaseCreationReqMsgType,
	}
}

func NewLeaseCreated(lease provider.Lease) leaseFSMMsg {
	return leaseFSMMsg{
		msgType:         leaseCreatedMsgType,
		leaseCreatedMsg: leaseCreatedMsg{lease: lease},
	}
}

func NewLeaseLost() leaseFSMMsg {
	return leaseFSMMsg{
		msgType: leaseLostMsgType,
	}
}

func NewLeadershipResignationReq(ctx context.Context, shard string) (leaseFSMMsg, <-chan error) {
	resp := make(chan error)
	msg := leaseFSMMsg{
		msgType: leadershipResignationReqMsgType,
		leadershipResignationReq: leadershipResignationReq{
			ctx:   ctx,
			shard: shard,
			resp:  resp,
		},
	}
	return msg, resp
}

func NewLeaseCloseMsg() (leaseFSMMsg, <-chan struct{}) {
	resp := make(chan struct{}, 1)
	return leaseFSMMsg{
		msgType:       leaseFSMCloseMsgType,
		leaseCloseMsg: leaseCloseMsg{resp: resp},
	}, resp
}

func NewConfirmationReqOnceLeader(shard string) leaseFSMMsg {
	return leaseFSMMsg{
		msgType: confirmationReqOnceLeaderMsgType,
		confirmationReqOnceLeader: confirmationReqOnceLeader{
			shard: shard,
		},
	}
}

type leaseFSMMsg struct {
	msgType leaseFSMMsgType
	// messages
	// external from end-customer
	leadershipRegistrationReq leadershipRegistrationReq
	leadershipResignationReq  leadershipResignationReq
	// from election FSM
	confirmationReqOnceLeader confirmationReqOnceLeader
	// internal
	leaseCreatedMsg leaseCreatedMsg
	leaseCloseMsg   leaseCloseMsg
}

type leaseFSMMsgType uint8

const (
	leadershipRegistrationReqMsgType leaseFSMMsgType = iota
	leaseCreationReqMsgType
	leaseCreatedMsgType
	leaseLostMsgType
	leadershipResignationReqMsgType
	confirmationReqOnceLeaderMsgType
	leaseFSMCloseMsgType
)

func (l leaseFSMMsgType) String() string {
	switch l {
	case leaseCreationReqMsgType:
		return "Lease Creation Request"
	case leaseCreatedMsgType:
		return "Lease Created Request"
	case leaseLostMsgType:
		return "Lease Lost"
	case leadershipResignationReqMsgType:
		return "Leadership Resignation Request"
	case confirmationReqOnceLeaderMsgType:
		return "Confirmation Request after Leadership"
	case leaseFSMCloseMsgType:
		return "Close"
	default:
		return "Leadership Registration Request"
	}
}

type leadershipRegistrationReq struct {
	shard string
	resp  chan<- LeadershipRegistrationResp
}

type leadershipRegistrationResp struct {
	leadershipAcquired <-chan struct{}
	leadershipLost     <-chan error
	err                error
}

func (r leadershipRegistrationResp) LeadershipAcquired() <-chan struct{} {
	return r.leadershipAcquired
}

func (r leadershipRegistrationResp) LeadershipLost() <-chan error {
	return r.leadershipLost
}

func (r leadershipRegistrationResp) Err() error {
	return r.err
}

type leaseCreatedMsg struct {
	lease provider.Lease
}

type leadershipResignationReq struct {
	ctx   context.Context
	shard string
	resp  chan<- error
}

type leaseCloseMsg struct {
	resp chan<- struct{}
}

type confirmationReqOnceLeader struct {
	shard string
}

package fsm

import (
	"context"

	"github.com/arunsworld/distributed/provider"
)

type LeaseFSM interface {
	Publish(leaseFSMMsg)
}

func NewLeaseFSM(provider provider.LeaseProvider, emdp provider.ElectionMetaDataProvider) *leaseFSM {
	result := &leaseFSM{
		provider: provider,
		emdp:     emdp,
		mailbox:  make(chan leaseFSMMsg, 30),
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

type leaseFSMMsg struct {
	msgType leaseFSMMsgType
	// messages
	leadershipRegistrationReq leadershipRegistrationReq
	leadershipResignationReq  leadershipResignationReq
	leaseCreatedMsg           leaseCreatedMsg
	leaseCloseMsg             leaseCloseMsg
}

type leaseFSMMsgType uint8

const (
	leadershipRegistrationReqMsgType leaseFSMMsgType = iota
	leaseCreationReqMsgType
	leaseCreatedMsgType
	leaseLostMsgType
	leadershipResignationReqMsgType
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

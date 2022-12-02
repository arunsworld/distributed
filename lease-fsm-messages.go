package distributed

import (
	"context"
	"fmt"
	"log"
)

type leaseFSMMsg struct {
	msgType leaseFSMMsgType
	payload any
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
	resp  chan<- leadershipRegistrationResp
}

type leadershipRegistrationResp struct {
	leadershipAcquired <-chan struct{}
	leadershipLost     <-chan error
	err                error
}

type leadershipResignationReq struct {
	ctx   context.Context
	shard string
	resp  chan<- error
}

func (lsfm *leaseFSM) processLeadershipRegistrationReq(shard string) leadershipRegistrationResp {
	if _, ok := lsfm.shards[shard]; ok {
		return leadershipRegistrationResp{
			err: fmt.Errorf("%s already registered, ignoring", shard),
		}
	}
	leadershipAcquired := make(chan struct{})
	leadershipLost := make(chan error, 1)
	lsfm.shards[shard] = newElectionFSM(shard, leadershipAcquired, leadershipLost, lsfm.emdp)

	switch lsfm.leaseState {
	case noLease:
		select {
		case lsfm.mailbox <- leaseFSMMsg{msgType: leaseCreationReqMsgType}:
		default:
		}
	case leaseAcquired:
		lsfm.shards[shard].registerLease(lsfm.lease)
	}

	return leadershipRegistrationResp{
		leadershipAcquired: leadershipAcquired,
		leadershipLost:     leadershipLost,
	}
}

func (lsfm *leaseFSM) processLeaseCreationReq() {
	if lsfm.leaseState != noLease {
		return
	}
	lsfm.leaseState = acquiringLease
	ctx, cancel := context.WithCancel(context.Background())
	lsfm.cancelLeaseAquisition = cancel
	go acquireLease(ctx, lsfm.provider, lsfm.mailbox)
}

func (lsfm *leaseFSM) activateShards() {
	for _, s := range lsfm.shards {
		s.registerLease(lsfm.lease)
	}
}

func (lsfm *leaseFSM) deactivateShardsDueToLeaseLoss() {
	for name, s := range lsfm.shards {
		if s.leaseLost() {
			delete(lsfm.shards, name)
		}
	}
}

func (lsfm *leaseFSM) resignLeadership(ctx context.Context, shard string) error {
	s, ok := lsfm.shards[shard]
	if !ok {
		return fmt.Errorf("no election leadership for %s found", shard)
	}
	_resp := make(chan resignationRespMsg)
	s.mailbox <- electionFSMMsg{
		msgType: resignMsgType,
		payload: resignationReqMsg{
			ctx:  ctx,
			resp: _resp,
		},
	}
	resp := <-_resp
	if resp.discardElection {
		delete(lsfm.shards, shard)
	}
	return resp.err
}

func (lsfm *leaseFSM) processClose() {
	if lsfm.leaseState == noLease {
		return
	}
	if lsfm.leaseState == acquiringLease {
		lsfm.cancelLeaseAquisition()
		return
	}
	resonsesToWaitFor := []chan struct{}{}
	for _, s := range lsfm.shards {
		resp := make(chan struct{})
		s.mailbox <- electionFSMMsg{
			msgType: closeMsgType,
			payload: resp,
		}
		resonsesToWaitFor = append(resonsesToWaitFor, resp)
	}
	for _, resp := range resonsesToWaitFor {
		<-resp
	}
	lsfm.lease.Close()
}

func acquireLease(ctx context.Context, provider LeaseProvider, mailbox chan leaseFSMMsg) {
	lease, err := provider.AcquireLease(ctx)
	if err != nil {
		select {
		case <-ctx.Done():
		default:
			log.Printf("WARNING: possible bug in acquireLease. received error but context isn't cancelled: %v", err)
		}
		return
	}
	mailbox <- leaseFSMMsg{msgType: leaseCreatedMsgType, payload: lease}

	<-lease.Expired()

	mailbox <- leaseFSMMsg{msgType: leaseLostMsgType}
}

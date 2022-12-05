package fsm

import (
	"context"
	"fmt"
	"log"

	"github.com/arunsworld/distributed/provider"
)

type leaseFSM struct {
	provider provider.LeaseProvider
	emdp     provider.ElectionMetaDataProvider
	// mailbox
	mailbox chan leaseFSMMsg // change to etcd free message type laster
	// internal state
	leaseState            leaseState
	shards                map[string]ElectionFSM
	lease                 provider.Lease
	cancelLeaseAquisition context.CancelFunc
}

type leaseState uint8

const (
	noLease leaseState = iota
	acquiringLease
	leaseAcquired
)

func (l leaseState) String() string {
	switch l {
	case acquiringLease:
		return "Acquiring Lease"
	case leaseAcquired:
		return "Lease Acquired"
	default:
		return "No Lease"
	}
}

func (lsfm *leaseFSM) Publish(msg leaseFSMMsg) {
	lsfm.mailbox <- msg
}

func (lsfm *leaseFSM) run() {
	lsfm.shards = make(map[string]ElectionFSM)
	for msg := range lsfm.mailbox {
		startingState := lsfm.leaseState
		switch msg.msgType {
		case leadershipRegistrationReqMsgType:
			msg.leadershipRegistrationReq.resp <- lsfm.processLeadershipRegistrationReq(msg.leadershipRegistrationReq.shard)
		case leaseCreationReqMsgType:
			lsfm.processLeaseCreationReq()
		case leaseCreatedMsgType:
			lsfm.leaseState = leaseAcquired
			lsfm.lease = msg.leaseCreatedMsg.lease
			lsfm.activateShards()
		case leaseLostMsgType:
			lsfm.deactivateShardsDueToLeaseLoss()
			go lsfm.lease.Close() // Note: this takes TTL seconds. Hence putting into background.
			lsfm.lease = nil
			lsfm.leaseState = noLease
			lsfm.mailbox <- leaseFSMMsg{msgType: leaseCreationReqMsgType}
		case leadershipResignationReqMsgType:
			req := msg.leadershipResignationReq
			req.resp <- lsfm.resignLeadership(req.ctx, req.shard)
		case leaseFSMCloseMsgType:
			lsfm.processClose()
			close(msg.leaseCloseMsg.resp)
			log.Printf("LEASE FSM: %s. Quitting.", msg.msgType)
			return
		}
		log.Printf("LEASE FSM: %s. %s -> %s.", msg.msgType, startingState, lsfm.leaseState)
	}
}

func (lsfm *leaseFSM) processLeadershipRegistrationReq(shard string) leadershipRegistrationResp {
	if _, ok := lsfm.shards[shard]; ok {
		return leadershipRegistrationResp{
			err: fmt.Errorf("%s already registered, ignoring", shard),
		}
	}
	leadershipAcquired := make(chan struct{})
	leadershipLost := make(chan error, 1)
	lsfm.shards[shard] = NewElectionFSM(shard, leadershipAcquired, leadershipLost, lsfm.emdp)

	switch lsfm.leaseState {
	case noLease:
		select {
		case lsfm.mailbox <- leaseFSMMsg{msgType: leaseCreationReqMsgType}:
		default:
		}
	case leaseAcquired:
		msg, resp := NewRegisterLeaseMessage(lsfm.lease)
		lsfm.shards[shard].Publish(msg)
		<-resp
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
	go acquireLease(ctx, lsfm.provider, lsfm)
}

func (lsfm *leaseFSM) activateShards() {
	for _, s := range lsfm.shards {
		msg, resp := NewRegisterLeaseMessage(lsfm.lease)
		s.Publish(msg)
		<-resp
	}
}

func (lsfm *leaseFSM) deactivateShardsDueToLeaseLoss() {
	for name, s := range lsfm.shards {
		msg, resp := NewCaptureLeaseLostMsg()
		s.Publish(msg)
		if <-resp {
			delete(lsfm.shards, name)
		}
	}
}

func (lsfm *leaseFSM) resignLeadership(ctx context.Context, shard string) error {
	s, ok := lsfm.shards[shard]
	if !ok {
		return fmt.Errorf("no election leadership for %s found", shard)
	}
	msg, _resp := NewResignationReqMsg(ctx)
	s.Publish(msg)
	resp := <-_resp
	if resp.DiscardElection() {
		delete(lsfm.shards, shard)
	}
	return resp.Err()
}

func (lsfm *leaseFSM) processClose() {
	if lsfm.leaseState == noLease {
		return
	}
	if lsfm.leaseState == acquiringLease {
		lsfm.cancelLeaseAquisition()
		return
	}
	resonsesToWaitFor := []<-chan struct{}{}
	for _, s := range lsfm.shards {
		msg, resp := NewElectionCloseMsg()
		s.Publish(msg)
		resonsesToWaitFor = append(resonsesToWaitFor, resp)
	}
	for _, resp := range resonsesToWaitFor {
		<-resp
	}
	lsfm.lease.Close()
}

func acquireLease(ctx context.Context, provider provider.LeaseProvider, lsfm LeaseFSM) {
	lease, err := provider.AcquireLease(ctx)
	if err != nil {
		select {
		case <-ctx.Done():
		default:
			log.Printf("WARNING: possible bug in acquireLease. received error but context isn't cancelled: %v", err)
		}
		return
	}
	lsfm.Publish(NewLeaseCreated(lease))

	<-lease.Expired()

	lsfm.Publish(NewLeaseLost())
}

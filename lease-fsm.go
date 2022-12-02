package distributed

import (
	"context"
	"log"
	"time"
)

func newLeaseFSM(provider LeaseProvider, emdp electionMetaDataProvider) *leaseFSM {
	result := &leaseFSM{
		provider: provider,
		emdp:     emdp,
		mailbox:  make(chan leaseFSMMsg, 30),
	}
	go result.run()
	return result
}

type electionMetaDataProvider interface {
	// identifying the locking key during election - the lowest denominator of lock
	constituency(shard string) string
	// value that is used during campaigning for leadership
	campaignPromise() string
	// value that is used as a part of a heartbeat during leadership
	leadershipReassurance(electedAt time.Time) string
}

type leaseFSM struct {
	provider LeaseProvider
	emdp     electionMetaDataProvider
	// mailbox
	mailbox chan leaseFSMMsg // change to etcd free message type laster
	// internal state
	leaseState            leaseState
	shards                map[string]*electionFSM
	lease                 Lease
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

func (lsfm *leaseFSM) run() {
	lsfm.shards = make(map[string]*electionFSM)
	for msg := range lsfm.mailbox {
		startingState := lsfm.leaseState
		switch msg.msgType {
		case leadershipRegistrationReqMsgType:
			req := msg.payload.(leadershipRegistrationReq)
			req.resp <- lsfm.processLeadershipRegistrationReq(req.shard)
		case leaseCreationReqMsgType:
			lsfm.processLeaseCreationReq()
		case leaseCreatedMsgType:
			lease := msg.payload.(Lease)
			lsfm.leaseState = leaseAcquired
			lsfm.lease = lease
			lsfm.activateShards()
		case leaseLostMsgType:
			lsfm.deactivateShardsDueToLeaseLoss()
			go lsfm.lease.Close() // Note: this takes TTL seconds. Hence putting into background.
			lsfm.lease = nil
			lsfm.leaseState = noLease
			lsfm.mailbox <- leaseFSMMsg{msgType: leaseCreationReqMsgType}
		case leadershipResignationReqMsgType:
			req := msg.payload.(leadershipResignationReq)
			req.resp <- lsfm.resignLeadership(req.ctx, req.shard)
		case leaseFSMCloseMsgType:
			lsfm.processClose()
			close(msg.payload.(chan struct{}))
			log.Printf("LEASE FSM: %s. Quitting.", msg.msgType)
			return
		}
		log.Printf("LEASE FSM: %s. %s -> %s.", msg.msgType, startingState, lsfm.leaseState)
		// log.Printf("LEASE FSM: %s. %s -> %s. Payload: %#v", msg.msgType, startingState, lsfm.leaseState, msg.payload)
	}
}

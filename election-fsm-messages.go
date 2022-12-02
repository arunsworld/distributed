package distributed

import (
	"context"
	"fmt"
	"log"
	"time"
)

type electionFSMMsg struct {
	msgType electionFSMMsgType
	payload any
}

type electionFSMMsgType uint8

const (
	registerLeaseMsgType electionFSMMsgType = iota
	electionCampaignFailed
	electedLeaderMsgType
	captureLeaseLostMsgType
	resignMsgType
	closeMsgType
)

func (e electionFSMMsgType) String() string {
	switch e {
	case electionCampaignFailed:
		return "Election Campaign Failed"
	case electedLeaderMsgType:
		return "Elected Leader"
	case captureLeaseLostMsgType:
		return "Lease Lost"
	case resignMsgType:
		return "Resign"
	case closeMsgType:
		return "Close"
	default:
		return "Register Lease"
	}
}

type registerLeaseMsg struct {
	lease Lease
	resp  chan<- struct{}
}

type electedLeaderMsg struct {
	electedAt time.Time
	election  Election
}

type resignationReqMsg struct {
	ctx  context.Context
	resp chan<- resignationRespMsg
}

type resignationRespMsg struct {
	err             error
	discardElection bool
}

func (efsm *electionFSM) processElection(lease Lease) {
	if efsm.electionState != notACandidate {
		log.Printf("WARNING: asked to process election for constituency: %s but we've already entered into an election", efsm.emdp.constituency(efsm.shard))
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	efsm.cancelElectionInProgress = cancel
	go campaignForLeadership(ctx, efsm.shard, efsm.emdp, lease, efsm.mailbox)
}

func (efsm *electionFSM) processLeaseLost() bool {
	switch efsm.electionState {
	case electedLeader:
		efsm.leadershipLost <- fmt.Errorf("lease lost")
		return true
	case electionCampaignInProgress:
		efsm.cancelElectionInProgress()
		return false
	case notACandidate:
		log.Printf("WARNING: Shard: %s received lease lost but it wasn't even a candidate", efsm.shard)
		return false
	}
	return false
}

func (efsm *electionFSM) processClose() {
	switch efsm.electionState {
	case electedLeader:
		ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
		efsm.election.Resign(ctx)
		cancel()
	case electionCampaignInProgress:
		efsm.cancelElectionInProgress()
	}
}

func campaignForLeadership(ctx context.Context, shard string, emdp electionMetaDataProvider, lease Lease, mailbox chan electionFSMMsg) {
	election := lease.ElectionFor(emdp.constituency(shard))
	if err := election.Campaign(ctx, emdp.campaignPromise()); err != nil {
		select {
		case <-ctx.Done():
		default:
			mailbox <- electionFSMMsg{
				msgType: electionCampaignFailed,
				payload: fmt.Errorf("error campaigning in election: %v", err),
			}
		}
		return
	}
	electedAt := time.Now()
	if err := reassureLeadership(ctx, emdp, electedAt, election); err != nil {
		mailbox <- electionFSMMsg{
			msgType: electionCampaignFailed,
			payload: err,
		}
		return
	}
	mailbox <- electionFSMMsg{
		msgType: electedLeaderMsgType,
		payload: electedLeaderMsg{
			electedAt: electedAt,
			election:  election,
		},
	}
}

func reassureLeadership(ctx context.Context, emdp electionMetaDataProvider, electedAt time.Time, election Election) error {
	if err := election.ReassureLeadership(ctx, emdp.leadershipReassurance(electedAt)); err != nil {
		return fmt.Errorf("error reassuring leadership: %v", err)
	}
	return nil
}

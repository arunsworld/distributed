package fsm

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/arunsworld/distributed/provider"
)

type electionFSM struct {
	shard              string
	emdp               provider.ElectionMetaDataProvider
	leadershipAcquired chan<- struct{}
	leadershipLost     chan<- error
	// mailbox
	mailbox chan electionFSMMsg
	// internal state
	currentLease             provider.Lease
	electionState            electionState
	cancelElectionInProgress context.CancelFunc
	electedLeaderAt          time.Time
	election                 provider.Election
}

type electionState uint8

const (
	noLeaseForElection electionState = iota
	electionCampaignInProgress
	electionCampaignFailed
	electedLeader
)

func (e electionState) String() string {
	switch e {
	case electionCampaignInProgress:
		return "Campaign in Progress"
	case electionCampaignFailed:
		return "Campaign Failed"
	case electedLeader:
		return "Elected Leader"
	default:
		return "No Lease for Election"
	}
}

func (efsm *electionFSM) Publish(msg electionFSMMsg) {
	efsm.mailbox <- msg
}

func (efsm *electionFSM) run() {
	var latestMsgType electionFSMMsgType
	defer func() {
		log.Printf("ELECTION FSM: %s. Quitting.", latestMsgType)
	}()
	for msg := range efsm.mailbox {
		latestMsgType = msg.msgType
		startingState := efsm.electionState
		switch msg.msgType {
		case registerLeaseMsgType:
			if efsm.electionState != noLeaseForElection {
				log.Printf("WARNING: asked to process election for constituency: %s but we already have a lease", efsm.emdp.Constituency(efsm.shard))
				return
			}
			efsm.processElection(msg.registerLeaseMsg.lease)
			close(msg.registerLeaseMsg.resp)
		case electionCampaignFailedMsgType:
			efsm.processFailedCampaign(msg.campaignFailedMsg.leaseID, msg.campaignFailedMsg.err)
		case electionCampaignRetryMsgType:
			efsm.processCampaignRetry(msg.campaignRetryMsg.leaseID)
		case electedLeaderMsgType:
			if efsm.electionState != electionCampaignInProgress {
				log.Printf("WARNING: leadership election message received when campaign not in progress. Ignorning.")
				continue
			}
			electionMsg := msg.electedLeaderMsg
			efsm.electedLeaderAt = electionMsg.electedAt
			efsm.election = electionMsg.election
			efsm.electionState = electedLeader
			close(efsm.leadershipAcquired)
		case captureLeaseLostMsgType:
			if efsm.processLeaseLostAndQuit() {
				msg.captureLeaseLostMsg.discardElection <- true
				return
			} else {
				efsm.currentLease = nil
				efsm.electionState = noLeaseForElection
				msg.captureLeaseLostMsg.discardElection <- false
			}
		case resignMsgType:
			req := msg.resignationReqMsg
			if efsm.electionState != electedLeader {
				req.resp <- resignationRespMsg{err: fmt.Errorf("error: cannot resign %s since not a leader", efsm.emdp.Constituency(efsm.shard))}
			} else {
				req.resp <- resignationRespMsg{discardElection: true, err: efsm.election.Resign(req.ctx)}
				return
			}
		case electionCloseMsgType:
			efsm.processClose()
			close(msg.electionCloseMsg.resp)
			return
		}
		log.Printf("ELECTION FSM: %s. %s -> %s.", msg.msgType, startingState, efsm.electionState)
	}
}

func (efsm *electionFSM) processElection(lease provider.Lease) {
	efsm.electionState = electionCampaignInProgress
	efsm.currentLease = lease
	ctx, cancel := context.WithCancel(context.Background())
	efsm.cancelElectionInProgress = cancel
	go campaignForLeadership(ctx, efsm.shard, efsm.emdp, lease, efsm)
}

func (efsm *electionFSM) processFailedCampaign(leaseID string, err error) {
	if !(efsm.electionState == electionCampaignInProgress || efsm.electionState == electionCampaignFailed) {
		log.Printf("WARNING: got message of old campaign (wrong state) failing for: %s: %v", efsm.emdp.Constituency(efsm.shard), err)
		return
	}
	if leaseID != efsm.currentLease.ID() {
		log.Printf("WARNING: got message of old campaign (because lease ID didn't match) failing for: %s: %v", efsm.emdp.Constituency(efsm.shard), err)
		return
	}
	log.Printf("warning: campaign for %s failed: %v", efsm.emdp.Constituency(efsm.shard), err)
	efsm.electionState = electionCampaignFailed
	go func() {
		<-time.After(time.Second)
		efsm.Publish(NewElectionCampaignRetryMsg(leaseID))
	}()
}

func (efsm *electionFSM) processCampaignRetry(leaseID string) {
	if efsm.electionState != electionCampaignFailed {
		log.Printf("will not retry failed election campaign since we're not in a failed campaign but in: %s", efsm.electionState.String())
		return
	}
	if efsm.currentLease.ID() != leaseID {
		log.Println("asked to retry stale election, ignoring")
		return
	}
	efsm.processElection(efsm.currentLease)
}

func (efsm *electionFSM) processLeaseLostAndQuit() bool {
	switch efsm.electionState {
	case electedLeader:
		efsm.leadershipLost <- fmt.Errorf("lease lost")
		return true
	case electionCampaignInProgress:
		efsm.cancelElectionInProgress()
		return false
	case noLeaseForElection:
		log.Printf("WARNING: Shard: %s received lease lost but it wasn't even a candidate", efsm.shard)
		return false
	}
	return false
}

func (efsm *electionFSM) processClose() {
	switch efsm.electionState {
	case electedLeader:
		ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
		if err := efsm.election.Resign(ctx); err != nil {
			log.Printf("WARNING: error when resigning during close: %v", err)
		}
		cancel()
	case electionCampaignInProgress:
		efsm.cancelElectionInProgress()
	}
}

func campaignForLeadership(ctx context.Context, shard string, emdp provider.ElectionMetaDataProvider, lease provider.Lease, efsm ElectionFSM) {
	election := lease.ElectionFor(emdp.Constituency(shard))
	if err := election.Campaign(ctx, emdp.CampaignPromise()); err != nil {
		select {
		case <-ctx.Done():
			log.Printf("campaign for leadership for: %s cancelled", emdp.Constituency(shard))
		default:
			efsm.Publish(NewCampaignFailedMsg(lease.ID(), fmt.Errorf("error campaigning in election: %v", err)))
		}
		return
	}
	electedAt := time.Now()
	if err := reassureLeadership(ctx, emdp, electedAt, election); err != nil {
		efsm.Publish(NewCampaignFailedMsg(lease.ID(), fmt.Errorf("error reassuring leadership while campaigning in election: %v", err)))
		return
	}
	efsm.Publish(NewElectedLeaderMsg(electedAt, election))
}

func reassureLeadership(ctx context.Context, emdp provider.ElectionMetaDataProvider, electedAt time.Time, election provider.Election) error {
	if err := election.ReassureLeadership(ctx, emdp.LeadershipReassurance(electedAt)); err != nil {
		return fmt.Errorf("error reassuring leadership: %v", err)
	}
	return nil
}

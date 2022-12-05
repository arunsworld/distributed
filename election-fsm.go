package distributed

import (
	"context"
	"fmt"
	"log"
	"time"
)

func newElectionFSM(shard string, leadershipAcquired chan<- struct{}, leadershipLost chan<- error, emdp electionMetaDataProvider) *electionFSM {
	result := &electionFSM{
		shard:              shard,
		emdp:               emdp,
		leadershipAcquired: leadershipAcquired,
		leadershipLost:     leadershipLost,
		mailbox:            make(chan electionFSMMsg, 30),
	}
	go result.run()
	return result
}

type electionFSM struct {
	shard              string
	emdp               electionMetaDataProvider
	leadershipAcquired chan<- struct{}
	leadershipLost     chan<- error
	// mailbox
	mailbox chan electionFSMMsg
	// internal state
	currentLeaseID           string
	currentLease             Lease
	electionState            electionState
	cancelElectionInProgress context.CancelFunc
	electedLeaderAt          time.Time
	election                 Election
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
				log.Printf("WARNING: asked to process election for constituency: %s but we already have a lease", efsm.emdp.constituency(efsm.shard))
				return
			}
			req := msg.payload.(registerLeaseMsg)
			efsm.processElection(req.lease)
			close(req.resp)
		case electionCampaignFailedMsgType:
			msg := msg.payload.(campaignFailedMsg)
			efsm.processFailedCampaign(msg.leaseID, msg.err)
		case electionCampaignRetryMsgType:
			msg := msg.payload.(campaignRetryMsg)
			efsm.processCampaignRetry(msg.leaseID)
		case electedLeaderMsgType:
			if efsm.electionState != electionCampaignInProgress {
				log.Printf("WARNING: leadership election message received when campaign not in progress. Ignorning.")
				continue
			}
			electionMsg := msg.payload.(electedLeaderMsg)
			efsm.electedLeaderAt = electionMsg.electedAt
			efsm.election = electionMsg.election
			efsm.electionState = electedLeader
			close(efsm.leadershipAcquired)
		case captureLeaseLostMsgType:
			if efsm.processLeaseLostAndQuit() {
				msg.payload.(chan bool) <- true
				return
			} else {
				efsm.currentLeaseID = ""
				efsm.electionState = noLeaseForElection
				msg.payload.(chan bool) <- false
			}
		case resignMsgType:
			req := msg.payload.(resignationReqMsg)
			if efsm.electionState != electedLeader {
				req.resp <- resignationRespMsg{err: fmt.Errorf("error: cannot resign %s since not a leader", efsm.emdp.constituency(efsm.shard))}
			} else {
				req.resp <- resignationRespMsg{discardElection: true, err: efsm.election.Resign(req.ctx)}
				return
			}
		case closeMsgType:
			resp := msg.payload.(chan struct{})
			efsm.processClose()
			close(resp)
			return
		}
		log.Printf("ELECTION FSM: %s. %s -> %s.", msg.msgType, startingState, efsm.electionState)
	}
}

// convenience APIs
func (efsm *electionFSM) registerLease(lease Lease) {
	resp := make(chan struct{})
	req := registerLeaseMsg{
		lease: lease,
		resp:  resp,
	}
	efsm.mailbox <- electionFSMMsg{
		msgType: registerLeaseMsgType,
		payload: req,
	}
	<-resp
}

func (efsm *electionFSM) leaseLost() bool {
	resp := make(chan bool)
	efsm.mailbox <- electionFSMMsg{
		msgType: captureLeaseLostMsgType,
		payload: resp,
	}
	return <-resp
}

package distributed

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/google/uuid"
)

type electionFSMMsg struct {
	msgType electionFSMMsgType
	payload any
}

type electionFSMMsgType uint8

const (
	registerLeaseMsgType electionFSMMsgType = iota
	electionCampaignFailedMsgType
	electionCampaignRetryMsgType
	electedLeaderMsgType
	captureLeaseLostMsgType
	resignMsgType
	closeMsgType
)

func (e electionFSMMsgType) String() string {
	switch e {
	case electionCampaignFailedMsgType:
		return "Election Campaign Failed"
	case electionCampaignRetryMsgType:
		return "Election Campaign Retry"
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

type campaignFailedMsg struct {
	leaseID string
	err     error
}

type campaignRetryMsg struct {
	leaseID string
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
	efsm.electionState = electionCampaignInProgress
	leaseID := uuid.New().String()
	efsm.currentLeaseID = leaseID
	efsm.currentLease = lease
	ctx, cancel := context.WithCancel(context.Background())
	efsm.cancelElectionInProgress = cancel
	go campaignForLeadership(ctx, efsm.shard, efsm.emdp, lease, leaseID, efsm.mailbox)
}

func (efsm *electionFSM) processFailedCampaign(leaseID string, err error) {
	if !(efsm.electionState == electionCampaignInProgress || efsm.electionState == electionCampaignFailed) {
		log.Printf("WARNING: got message of old campaign (wrong state) failing for: %s: %v", efsm.emdp.constituency(efsm.shard), err)
		return
	}
	if leaseID != efsm.currentLeaseID {
		log.Printf("WARNING: got message of old campaign (because lease ID didn't match) failing for: %s: %v", efsm.emdp.constituency(efsm.shard), err)
		return
	}
	log.Printf("warning: campaign for %s failed: %v", efsm.emdp.constituency(efsm.shard), err)
	efsm.electionState = electionCampaignFailed
	go func() {
		<-time.After(time.Second)
		efsm.mailbox <- electionFSMMsg{
			msgType: electionCampaignRetryMsgType,
			payload: campaignRetryMsg{
				leaseID: leaseID,
			},
		}
	}()
}

func (efsm *electionFSM) processCampaignRetry(leaseID string) {
	if efsm.electionState != electionCampaignFailed {
		log.Printf("will not retry failed election campaign since we're not in a failed campaign but in: %s", efsm.electionState.String())
		return
	}
	if efsm.currentLeaseID != leaseID {
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

func campaignForLeadership(ctx context.Context, shard string, emdp electionMetaDataProvider, lease Lease, leaseID string, mailbox chan electionFSMMsg) {
	election := lease.ElectionFor(emdp.constituency(shard))
	if err := election.Campaign(ctx, emdp.campaignPromise()); err != nil {
		select {
		case <-ctx.Done():
			log.Printf("campaign for leadership for: %s cancelled", emdp.constituency(shard))
		default:
			mailbox <- electionFSMMsg{
				msgType: electionCampaignFailedMsgType,
				payload: campaignFailedMsg{
					leaseID: leaseID,
					err:     fmt.Errorf("error campaigning in election: %v", err),
				},
			}
		}
		return
	}
	electedAt := time.Now()
	if err := reassureLeadership(ctx, emdp, electedAt, election); err != nil {
		mailbox <- electionFSMMsg{
			msgType: electionCampaignFailedMsgType,
			payload: campaignFailedMsg{
				leaseID: leaseID,
				err:     fmt.Errorf("error reassuring leadership while campaigning in election: %v", err),
			},
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

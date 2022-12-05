package fsm

import (
	"context"
	"time"

	"github.com/arunsworld/distributed/provider"
)

type ElectionFSM interface {
	Publish(electionFSMMsg)
}

func NewElectionFSM(shard string, leadershipAcquired chan<- struct{}, leadershipLost chan<- error, emdp provider.ElectionMetaDataProvider) ElectionFSM {
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

func NewRegisterLeaseMessage(lease provider.Lease) (electionFSMMsg, <-chan struct{}) {
	resp := make(chan struct{}, 1)
	return electionFSMMsg{
		msgType: registerLeaseMsgType,
		registerLeaseMsg: registerLeaseMsg{
			lease: lease,
			resp:  resp,
		},
	}, resp
}

func NewCampaignFailedMsg(leaseID string, err error) electionFSMMsg {
	return electionFSMMsg{
		msgType:           electionCampaignFailedMsgType,
		campaignFailedMsg: campaignFailedMsg{leaseID: leaseID, err: err},
	}
}

func NewElectedLeaderMsg(electedAt time.Time, election provider.Election) electionFSMMsg {
	return electionFSMMsg{
		msgType:          electedLeaderMsgType,
		electedLeaderMsg: electedLeaderMsg{electedAt: electedAt, election: election},
	}
}

func NewElectionCampaignRetryMsg(leaseID string) electionFSMMsg {
	return electionFSMMsg{
		msgType:          electionCampaignRetryMsgType,
		campaignRetryMsg: campaignRetryMsg{leaseID: leaseID},
	}
}

func NewCaptureLeaseLostMsg() (electionFSMMsg, <-chan bool) {
	resp := make(chan bool, 1)
	return electionFSMMsg{
		msgType: captureLeaseLostMsgType,
		captureLeaseLostMsg: captureLeaseLostMsg{
			discardElection: resp,
		},
	}, resp
}

func NewResignationReqMsg(ctx context.Context) (electionFSMMsg, <-chan ResignationResp) {
	resp := make(chan ResignationResp, 1)
	return electionFSMMsg{
		msgType: resignMsgType,
		resignationReqMsg: resignationReqMsg{
			ctx:  ctx,
			resp: resp,
		},
	}, resp
}

type ResignationResp interface {
	Err() error
	DiscardElection() bool
}

func NewElectionCloseMsg() (electionFSMMsg, <-chan struct{}) {
	resp := make(chan struct{}, 1)
	return electionFSMMsg{
		msgType: electionCloseMsgType,
		electionCloseMsg: electionCloseMsg{
			resp: resp,
		},
	}, resp
}

type electionFSMMsg struct {
	msgType electionFSMMsgType
	// messages
	registerLeaseMsg    registerLeaseMsg
	campaignFailedMsg   campaignFailedMsg
	campaignRetryMsg    campaignRetryMsg
	electedLeaderMsg    electedLeaderMsg
	captureLeaseLostMsg captureLeaseLostMsg
	resignationReqMsg   resignationReqMsg
	electionCloseMsg    electionCloseMsg
}

type electionFSMMsgType uint8

const (
	registerLeaseMsgType electionFSMMsgType = iota
	electionCampaignFailedMsgType
	electionCampaignRetryMsgType
	electedLeaderMsgType
	captureLeaseLostMsgType
	resignMsgType
	electionCloseMsgType
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
	case electionCloseMsgType:
		return "Close"
	default:
		return "Register Lease"
	}
}

type registerLeaseMsg struct {
	lease provider.Lease
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
	election  provider.Election
}

type resignationReqMsg struct {
	ctx  context.Context
	resp chan<- ResignationResp
}

type resignationRespMsg struct {
	err             error
	discardElection bool
}

func (r resignationRespMsg) Err() error {
	return r.err
}

func (r resignationRespMsg) DiscardElection() bool {
	return r.discardElection
}

type captureLeaseLostMsg struct {
	discardElection chan<- bool
}

type electionCloseMsg struct {
	resp chan<- struct{}
}

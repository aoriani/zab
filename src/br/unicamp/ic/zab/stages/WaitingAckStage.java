package br.unicamp.ic.zab.stages;

import java.util.HashMap;
import java.util.HashSet;

import org.apache.log4j.Logger;

import br.unicamp.ic.zab.Leader;
import br.unicamp.ic.zab.Packet;

/**
 * Packet in this stage are waiting for a quorum of acks to move to next stage
 * Because check for quorum only need to be done when a ack is arrived, no need
 * for this stage to be a thread.
 * @author andre
 *
 */
public class WaitingAckStage implements PipelineStage {
    private static final Logger LOG = Logger.getLogger(WaitingAckStage.class);


    private static class Proposal{
        public Packet proposal;
        public HashSet<Long> ackset;

        public Proposal (Packet proposal){
            this.proposal = proposal;
            this.ackset = new HashSet<Long>();
        }
    }

    HashMap<Long,Proposal> waitingQuorumProposal = new HashMap<Long,Proposal>();
    private PipelineStage nextStage;
    Leader leader;

    public WaitingAckStage(Leader leader, PipelineStage nextStage){
        this.leader = leader;
        this.nextStage = nextStage;
    }


    @Override
    public void receiveFromPreviousStage(Packet proposal)
            throws InterruptedException {
        LOG.debug("Received proposal to wait for quorum: " + proposal);

       //Add packet to table
        synchronized(waitingQuorumProposal){
            Proposal p = new Proposal(proposal);
            //FIXME When log is implemented ,
            //leader ack shall be added by other stage
            p.ackset.add(leader.getId());
            waitingQuorumProposal.put(proposal.getProposalID(), p);
        }
    }

    /**
     * Process an ack to a proposal. If quorum is reached , it is moved to next
     * stage. Ack for quorum that are no longer on this stage are ignored
     * @param proposalId the id of the acknowledged proposal
     * @param serverId the id of server that sent the ack
     * @throws InterruptedException
     */
    public void processAck(long proposalId, long serverId) throws InterruptedException{
        synchronized(waitingQuorumProposal){
            Proposal proposal = waitingQuorumProposal.get(proposalId);
            if(proposal != null){
                proposal.ackset.add(serverId);
                //Checking for quorum
                if(leader.getQuorumVerifier().containsQuorum(proposal.ackset)){
                    LOG.debug("Proposal " + proposalId +" has reached quorum");
                    waitingQuorumProposal.remove(proposalId);
                    nextStage.receiveFromPreviousStage(proposal.proposal);
                }
            }else{
                LOG.warn("Ack to proposal " + proposalId + "sent by " +
                        serverId + " is being ignored");
            }
        }
    }


    @Override
    public void shutdown() {
        nextStage.shutdown();
    }

}

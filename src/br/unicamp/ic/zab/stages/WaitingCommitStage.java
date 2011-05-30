package br.unicamp.ic.zab.stages;

import java.util.HashMap;

import org.apache.log4j.Logger;

import br.unicamp.ic.zab.Packet;

public class WaitingCommitStage implements PipelineStage {
    private static final Logger LOG = Logger.getLogger(WaitingCommitStage.class);

    private HashMap<Long,Packet> waitingCommitPackets = new HashMap<Long,Packet>();

    private PipelineStage nextStage;

    public WaitingCommitStage(PipelineStage nextStage){
        this.nextStage = nextStage;
    }

    @Override
    public void receiveFromPreviousStage(Packet proposal)
            throws InterruptedException {
        LOG.debug("Received proposal to wait for commit : " + proposal);
        synchronized (waitingCommitPackets) {
            waitingCommitPackets.put(proposal.getProposalId(), proposal);
        }
    }

    public void processCommit(long proposalId) throws InterruptedException{
        synchronized(waitingCommitPackets){
            Packet proposal = waitingCommitPackets.get(proposalId);
            if(proposal != null){
                LOG.debug("Commit proposal "+ proposalId);
                waitingCommitPackets.remove(proposalId);
                nextStage.receiveFromPreviousStage(proposal);
            }else{
                LOG.warn("Cannot commit. Proposal "+ proposalId + " not found");
            }
        }
    }

    @Override
    public void shutdown() {
        nextStage.shutdown();
    }

}

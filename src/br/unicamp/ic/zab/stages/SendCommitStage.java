package br.unicamp.ic.zab.stages;

import java.util.concurrent.LinkedBlockingQueue;

import org.apache.log4j.Logger;

import br.unicamp.ic.zab.Leader;
import br.unicamp.ic.zab.Packet;

public class SendCommitStage extends Thread implements PipelineStage {

    private static final Logger LOG = Logger.getLogger(SendCommitStage.class);

    private Leader leader;
    private LinkedBlockingQueue<Packet> tobeCommitted = new LinkedBlockingQueue<Packet>();
    private PipelineStage nextStage;

    public SendCommitStage(Leader leader, PipelineStage nextStage){
        super("SendCommiStage");
        this.leader = leader;
        this.nextStage = nextStage;
    }

    @Override
    public void receiveFromPreviousStage(Packet proposal) throws InterruptedException{
        LOG.debug("To be commited proposal: " + proposal);
        tobeCommitted.put(proposal);
    }

    @Override
    public void run() {

        try {
            while (true) {
                Packet proposal = tobeCommitted.take();
                if (proposal.getType() == Packet.Type.END_OF_STREAM) {
                    // Quit the thread since there won't be more work
                    break;
                }
                // Create the commit packet
                Packet commitPacket = Packet.createCommit(proposal
                        .getProposalId());
                leader.sendPacketToFollowers(commitPacket);
                nextStage.receiveFromPreviousStage(proposal);
            }
        } catch (InterruptedException e) {
            LOG.error("Unexpected exception causing shutdown", e);
        } finally {
            shutdown();
        }

    }

    @Override
    public void shutdown() {
        tobeCommitted.add(Packet.createEndOfStream());
        interrupt();
        nextStage.shutdown();
    }


}

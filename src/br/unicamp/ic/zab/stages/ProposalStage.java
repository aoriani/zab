package br.unicamp.ic.zab.stages;

import java.util.concurrent.LinkedBlockingQueue;

import org.apache.log4j.Logger;

import br.unicamp.ic.zab.Leader;
import br.unicamp.ic.zab.Packet;

/**
 * Packets in this stage are waiting to be proposed to followers
 * @author andre
 *
 */
public class ProposalStage extends Thread implements PipelineStage {
    private static final Logger LOG = Logger.getLogger(ProposalStage.class);

    private Leader leader;
    private LinkedBlockingQueue<Packet> tobeProposed = new LinkedBlockingQueue<Packet>();
    private PipelineStage nextStage;

    public ProposalStage(Leader leader, PipelineStage nextStage){
        super("ProposalStage");
        this.leader = leader;
        this.nextStage = nextStage;
    }

    @Override
    public void receiveFromPreviousStage(Packet proposal) throws InterruptedException{
        //Sanity check
        if(proposal.getType() != Packet.Type.PROPOSAL){
            throw new IllegalArgumentException("Only proposal packets are allowed in the pipeline");
        }
        LOG.debug("To be sent proposal: " + proposal);
        tobeProposed.put(proposal);
    }

    @Override
    public void run() {

        try {
            while (true) {
                Packet proposal = tobeProposed.take();
                if (proposal.getType() == Packet.Type.END_OF_STREAM) {
                    // Quit the thread since there won't be more work
                    break;
                }
                leader.sendPacketToFollowers(proposal);
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
        tobeProposed.add(Packet.createEndOfStream());
        interrupt();
        nextStage.shutdown();

    }

}

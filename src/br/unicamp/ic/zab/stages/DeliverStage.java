package br.unicamp.ic.zab.stages;

import java.util.concurrent.LinkedBlockingQueue;

import org.apache.log4j.Logger;

import br.unicamp.ic.zab.Leader;
import br.unicamp.ic.zab.Packet;

public class DeliverStage extends Thread implements PipelineStage {
//TODO: Deliver , Commit and proposal stages are candidates to factorization
    private static final Logger LOG = Logger.getLogger(DeliverStage.class);

    private Leader leader;
    private LinkedBlockingQueue<Packet> tobeDelivered = new LinkedBlockingQueue<Packet>();


    public DeliverStage(Leader leader){
        this.leader = leader;
    }

    @Override
    public void receiveFromPreviousStage(Packet proposal) throws InterruptedException{
        LOG.debug("To be delivered proposal: " + proposal);
        tobeDelivered.put(proposal);
    }

    @Override
    public void run() {

        try {
            while (true) {
                Packet proposal = tobeDelivered.take();
                if (proposal.getType() == Packet.Type.END_OF_STREAM) {
                    LOG.info("Packet of death");
                    // Quit the thread since there won't be more work
                    break;
                }
                LOG.debug("Delivering proposal " + proposal.getProposalID());
                leader.deliver(proposal.getPayload());
            }

        } catch (InterruptedException e) {
            LOG.error("Unexpected exception causing shutdown", e);
        } finally {
            shutdown();
        }

    }

    @Override
    public void shutdown() {
       tobeDelivered.add(Packet.createEndOfStream());
       interrupt();
    }


}

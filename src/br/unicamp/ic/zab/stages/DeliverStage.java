package br.unicamp.ic.zab.stages;

import java.util.concurrent.LinkedBlockingQueue;

import org.apache.log4j.Logger;

import br.unicamp.ic.zab.Packet;
import br.unicamp.ic.zab.PeerState;

public class DeliverStage extends Thread implements PipelineStage {
//TODO: Deliver , Commit and proposal stages are candidates to factorization
    private static final Logger LOG = Logger.getLogger(DeliverStage.class);

    private PeerState peerStage;
    private LinkedBlockingQueue<Packet> tobeDelivered = new LinkedBlockingQueue<Packet>();


    public DeliverStage(PeerState leader){
        super("DeliverStage");
        this.peerStage = leader;
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
                LOG.debug("Delivering proposal " + proposal.getProposalId());
                peerStage.deliver(proposal.getPayload());
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

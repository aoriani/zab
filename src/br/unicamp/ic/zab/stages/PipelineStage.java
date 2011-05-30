package br.unicamp.ic.zab.stages;

import br.unicamp.ic.zab.Packet;

/**
 * Interface for a stage of the proposal pipeline. Each stage represents a stage
 * a proposal may be : to be proposed, waiting ack, waiting commit, ...
 * @author andre
 *
 */
public interface PipelineStage {
    /**
     * Pass the packet to the  next stage in the pipeline
     * @param proposal the proposal packet to be sent to next stage.
     * @throws InterruptedException
     */
    public void receiveFromPreviousStage(Packet proposal) throws InterruptedException;

    /**
     * Forces a stage to stop and free resources
     */
    public void shutdown();

}

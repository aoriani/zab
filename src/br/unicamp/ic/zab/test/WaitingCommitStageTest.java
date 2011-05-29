package br.unicamp.ic.zab.test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

import br.unicamp.ic.zab.Packet;
import br.unicamp.ic.zab.stages.PipelineStage;
import br.unicamp.ic.zab.stages.WaitingCommitStage;


/**
 * This class tests WaitingCommitStage
 * @author andre
 *
 */
public class WaitingCommitStageTest {

    /**
     * Dummy class only to verify if methods were called
     * @author andre
     *
     */
    final static class DummyStage implements PipelineStage{
        boolean receiveFromPreviousStageCalled = false;
        boolean shutdownCalled = false;

        @Override
        public void receiveFromPreviousStage(Packet proposal)
                throws InterruptedException {
            receiveFromPreviousStageCalled = true;
        }

        @Override
        public void shutdown() {
            shutdownCalled = true;
        }
    }

    /**
     * This test of shutdown is called on next stage
     */
    @Test
    public void testShutdown(){
        DummyStage dummyStage = new DummyStage();
        WaitingCommitStage stage = new WaitingCommitStage(dummyStage);
        stage.shutdown();
        assertTrue("Shutdown for next stage should be called", dummyStage.shutdownCalled);
    }

    /**
     * Tests if a proposal waiting for a commit is moved to next stage
     * when commit arrives
     * @throws InterruptedException
     */
    @Test
    public void testCommitedProposalMovingToNextStage() throws InterruptedException{
        DummyStage dummyStage = new DummyStage();
        WaitingCommitStage stage = new WaitingCommitStage(dummyStage);

        Packet proposal = Packet.createProposal(1, new byte[0]);
        stage.receiveFromPreviousStage(proposal);
        stage.processCommit(1);
        assertTrue("Proposal should be moved to next stage",dummyStage.receiveFromPreviousStageCalled);
    }

    @Test
    public void testIgnoreUnknownProposal() throws InterruptedException{
        DummyStage dummyStage = new DummyStage();
        WaitingCommitStage stage = new WaitingCommitStage(dummyStage);

        stage.processCommit(1);
        assertFalse("Proposal should not be moved to next stage",dummyStage.receiveFromPreviousStageCalled);
    }

}

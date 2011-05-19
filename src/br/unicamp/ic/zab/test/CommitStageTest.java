package br.unicamp.ic.zab.test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;

import mockit.Mock;
import mockit.MockUp;
import mockit.UsingMocksAndStubs;

import org.junit.Test;

import br.unicamp.ic.zab.Leader;
import br.unicamp.ic.zab.Packet;
import br.unicamp.ic.zab.stages.CommitStage;
import br.unicamp.ic.zab.stages.PipelineStage;

/**
 * Tests CommitStage
 * @author andre
 *
 */
@UsingMocksAndStubs({Leader.class})
public class CommitStageTest {

    /**
     * Dummy PipelineStage, only to test if shutdown was called
     * @author andre
     *
     */
    private static class DummyStage implements PipelineStage {
        public boolean shutdownCalled = false;

        @Override
        public void receiveFromPreviousStage(Packet proposal)
                throws InterruptedException {
        }

        @Override
        public void shutdown() {
            shutdownCalled = true;

        }
    }

    /**
     * Test if Commit can finish after just have started and if it shutdowns
     * the next stage
     * @throws IOException
     * @throws InterruptedException
     */

    @Test(timeout=5000)
    public void testSimpleShutdown() throws IOException, InterruptedException{
        DummyStage dummyStage = new DummyStage();
        CommitStage stage = new CommitStage(new Leader(null), dummyStage);
        stage.start();
        Thread.sleep(1000);
        stage.shutdown();
        stage.join();
        assertTrue("Shutdown for next stage should have been called", dummyStage.shutdownCalled);
    }

    /**
     * Test if commit stage can finish if interrupt is called and if in that cases
     * it shutdown the next stage
     * @throws IOException
     * @throws InterruptedException
     */
    @Test(timeout=5000)
    public void testInterrupt() throws IOException, InterruptedException{
        DummyStage dummyStage = new DummyStage();
        CommitStage stage = new CommitStage(new Leader(null), dummyStage);
        stage.start();
        Thread.sleep(1000);
        stage.interrupt();
        stage.join();
        assertTrue("Shutdown for next stage should have been called", dummyStage.shutdownCalled);
    }

    /**
     * Two packet are sent from previous stage. Check if they are received in order,
     * if leader is called to deliver commit packets and next received the proposals
     * @throws IOException
     * @throws InterruptedException
     */
    @Test(timeout=5000)
    public void testDeliverTwoPackets() throws IOException, InterruptedException{

        final Packet p1 = Packet.createProposal(1, new byte[] {(byte)0xde,(byte)0xad});
        final Packet p2 = Packet.createProposal(2, new byte[] {(byte)0xbe,(byte)0xef});

        /**
         * Leader stub
         * @author andre
         *
         */
        new MockUp<Leader>(){
            int times = 0;

            @Mock(invocations = 2)
            public void sendPacketToFollowers(Packet packet){
                assertEquals("Packet should be a COMMIT",Packet.Type.COMMIT, packet.getType());
                ++times;
                switch(times){
                    case 1:
                        assertEquals("We should have received the first package first",
                                1, p1.getProposalID());
                    break;
                    case 2:
                        assertEquals("We should have received the second package second",
                                2, p2.getProposalID());
                    break;
                }
            }

        };

        class DummyStage2 implements PipelineStage {
            int times = 0;
            @Override
            public void receiveFromPreviousStage(Packet proposal)
                    throws InterruptedException {
                ++times;
                switch(times){
                    case 1:
                        assertEquals("We should have received the first package first",
                                    proposal, p1);
                    break;
                    case 2:
                        assertEquals("We should have received the second package second",
                                    proposal, p2);
                    break;
                }
            }

            @Override
            public void shutdown() {}
        }

        DummyStage2 dummyStage = new DummyStage2();
        CommitStage stage = new CommitStage(new Leader(null),dummyStage);
        stage.start();
        stage.receiveFromPreviousStage(p1);
        stage.receiveFromPreviousStage(p2);
        Thread.sleep(1000);
        stage.shutdown();
        stage.join();
        assertEquals("Two packets should have be sent to next stage", 2, dummyStage.times);

    }


}

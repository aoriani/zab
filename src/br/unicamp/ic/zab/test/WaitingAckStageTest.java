package br.unicamp.ic.zab.test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;

import mockit.Mock;
import mockit.MockUp;
import mockit.UsingMocksAndStubs;

import org.junit.Test;

import br.unicamp.ic.zab.Leader;
import br.unicamp.ic.zab.Packet;
import br.unicamp.ic.zab.QuorumVerifier;
import br.unicamp.ic.zab.stages.PipelineStage;
import br.unicamp.ic.zab.stages.WaitingAckStage;

/**
 * This class test WaitingAckStage
 * @author andre
 *
 */
@UsingMocksAndStubs({Leader.class})
public class WaitingAckStageTest {

    /**
     * Dummy PipelineStage, only to test the methods are called
     * @author andre
     *
     */
     static final class DummyStage implements PipelineStage {
         public boolean nextStageReceived = false;
         public boolean shutdownCalled = false;

         @Override
         public void receiveFromPreviousStage(Packet proposal)
                 throws InterruptedException {
             nextStageReceived = true;
         }

         @Override
         public void shutdown(){
             shutdownCalled = true;
         }
     }

     /**
      * Leader stub
      * @author andre
      *
      */
      public static final class MockedLeaderConstructor extends MockUp<Leader>{

         QuorumVerifier qv = new QuorumVerifier(5); //ensemble of 5 servers
         @Mock
         public Long getId(){
             return 1l;
         }

         @Mock
         public QuorumVerifier getQuorumVerifier(){
             return qv;
         }
     }

    @Test
    public void testShutdownCalledForNextStage() throws IOException{
        //Setup Mock
        new MockedLeaderConstructor();

        DummyStage dummyStage = new DummyStage();
        WaitingAckStage stage = new WaitingAckStage(new Leader(null),dummyStage);
        stage.shutdown();
        assertTrue("Shutdown for next stage should be called", dummyStage.shutdownCalled);
    }

    @Test
    public void testProposalReachesQuorum() throws IOException, InterruptedException{
        //Setup Mock
        new MockedLeaderConstructor();


        DummyStage dummyStage = new DummyStage();
        WaitingAckStage stage = new WaitingAckStage(new Leader(null), dummyStage);
        Packet proposal = Packet.createProposal(1, new byte[] {(byte)0xbe,(byte)0xef});
        stage.receiveFromPreviousStage(proposal);
        assertFalse("Quorum should not have been reached",dummyStage.nextStageReceived);
        stage.processAck(1, 2);
        assertFalse("Quorum should not have been reached",dummyStage.nextStageReceived);
        stage.processAck(1, 3);
        assertTrue("Quorum should have been reached",dummyStage.nextStageReceived);
    }

    @Test
    public void testIgnoresProposalNotProposed() throws IOException, InterruptedException{
        //Setup Mock
        new MockedLeaderConstructor();

        DummyStage dummyStage = new DummyStage();
        WaitingAckStage stage = new WaitingAckStage(new Leader(null), dummyStage);
        assertFalse("Quorum should not have been reached",dummyStage.nextStageReceived);
        stage.processAck(1, 2);
        assertFalse("Quorum should not have been reached",dummyStage.nextStageReceived);
        stage.processAck(1, 3);
        assertFalse("Quorum should not have been reached",dummyStage.nextStageReceived);
    }


}

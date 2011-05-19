package br.unicamp.ic.zab.test;

import static org.junit.Assert.*;

import java.io.IOException;

import org.junit.Test;

import mockit.Mock;
import mockit.MockUp;
import mockit.UsingMocksAndStubs;
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


    @Test
    public void testProposalReachesQuorum() throws IOException, InterruptedException{

        /**
        * Dummy PipelineStage, only to test if the next stage receives the
        * proposal once it reaches quorum
        * @author andre
        *
        */
        class DummyStage implements PipelineStage {
            public boolean nextStageReceived = false;

            @Override
            public void receiveFromPreviousStage(Packet proposal)
                    throws InterruptedException {
                nextStageReceived = true;
            }

            @Override
            public void shutdown() {}
        };

        /**
         * Leader stub
         * @author andre
         *
         */
        new MockUp<Leader>(){

            QuorumVerifier qv = new QuorumVerifier(5); //ensemble of 5 servers
            @Mock
            public Long getId(){
                return 1l;
            }

            @Mock
            public QuorumVerifier getQuorumVerifier(){
                return qv;
            }
        };

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

}

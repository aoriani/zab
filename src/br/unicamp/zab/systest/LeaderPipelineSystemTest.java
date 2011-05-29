package br.unicamp.zab.systest;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import java.io.IOException;

import mockit.Mock;
import mockit.MockUp;
import mockit.UsingMocksAndStubs;

import org.junit.Test;

import br.unicamp.ic.zab.Leader;
import br.unicamp.ic.zab.Packet;
import br.unicamp.ic.zab.QuorumVerifier;
import br.unicamp.ic.zab.stages.DeliverStage;
import br.unicamp.ic.zab.stages.ProposalStage;
import br.unicamp.ic.zab.stages.SendCommitStage;
import br.unicamp.ic.zab.stages.WaitingAckStage;

@UsingMocksAndStubs({Leader.class})
public class LeaderPipelineSystemTest {

    @Test
    public void leaderPipelineTest() throws IOException, InterruptedException{

        class MyInt {volatile int value = 0;};
        final MyInt stage= new MyInt();

        //Set up leader stub
        new MockUp<Leader>(){

            private QuorumVerifier qv = new QuorumVerifier(3);


            @Mock
            public void sendPacketToFollowers(Packet packet) throws InterruptedException {
                if(packet.getType() == Packet.Type.PROPOSAL){
                    stage.value = 1;
                }else if(packet.getType() == Packet.Type.COMMIT){
                    stage.value = (stage.value ==3)?4:-1;
                }else{
                    //Junit does not work with multi thread
                    // set a value so the thread under test can check for
                    // problems
                    stage.value = -1 ;
                }
            }

            @Mock
            public Long getId() {
                stage.value = (stage.value ==1)?2:-1;
                return 1L;
            }

            @Mock
            public QuorumVerifier getQuorumVerifier() {
                stage.value = (stage.value ==2 || stage.value ==3)?3:-1;//Because it is called twice
                return qv;
            }

            @Mock
            public void deliver(byte[] payload){
                stage.value = (stage.value ==4)?5:-1;
            }

        };

        //Set up pipeline;
        Leader leader = new Leader(null);
        DeliverStage deliverStage = new DeliverStage(leader);
        deliverStage.start();
        SendCommitStage sendCommitStage = new SendCommitStage(leader,deliverStage);
        sendCommitStage.start();
        WaitingAckStage waitingAckStage = new WaitingAckStage(leader,sendCommitStage);
        ProposalStage proposalStage = new ProposalStage(leader, waitingAckStage);
        proposalStage.start();


        //The test: see if things happens and in the expected order.
        Packet proposal = Packet.createProposal(1, new byte[0]);
        proposalStage.receiveFromPreviousStage(proposal);
        Thread.sleep(500);
        assertEquals("Leader should have received a proposal to send and added its ack",2,stage.value);
        waitingAckStage.processAck(1, 2);
        assertEquals("Quorum should have been verified",3,stage.value);
        waitingAckStage.processAck(1, 3);
        Thread.sleep(500);
        assertEquals("Proposal should have been delivered",5,stage.value);

        //Verify shutdown

        proposalStage.shutdown();
        proposalStage.join();
        Thread.sleep(500);
        assertFalse("SendCommitStage should be off", sendCommitStage.isAlive());
        assertFalse("DeliverStage should be off", deliverStage.isAlive());

    }
}

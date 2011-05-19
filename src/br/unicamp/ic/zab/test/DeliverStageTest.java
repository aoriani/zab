package br.unicamp.ic.zab.test;

import java.io.IOException;
import java.util.Arrays;

import mockit.Mock;
import mockit.MockUp;
import mockit.UsingMocksAndStubs;

import org.junit.Test;
import static org.junit.Assert.*;

import br.unicamp.ic.zab.Leader;
import br.unicamp.ic.zab.Packet;
import br.unicamp.ic.zab.stages.DeliverStage;

/**
 * Test the DeliverStage
 * @author andre
 *
 */
@UsingMocksAndStubs({Leader.class})
public class DeliverStageTest {


    /**
     * Test if DeliverStage can finish after just have started
     * @throws IOException
     * @throws InterruptedException
     */
    @Test(timeout=5000)
    public void testSimpleShutdown() throws IOException, InterruptedException{
        DeliverStage stage = new DeliverStage(new Leader(null));
        stage.start();
        Thread.sleep(1000);
        stage.shutdown();
        stage.join();

    }

    /**
     * Test if deliver stage can finish if interrupt is called
     * @throws IOException
     * @throws InterruptedException
     */
    @Test(timeout=5000)
    public void testInterrupt() throws IOException, InterruptedException{
        DeliverStage stage = new DeliverStage(new Leader(null));
        stage.start();
        Thread.sleep(1000);
        stage.interrupt();
        stage.join();
    }

    /**
     * Test of the payload of two incoming packets are delivered in order
     * @throws IOException
     * @throws InterruptedException
     */
    @Test(timeout=5000)
    public void testDeliverTwoPackets() throws IOException, InterruptedException{

        final Packet p1 = Packet.createProposal(1, new byte[] {(byte)0xde,(byte)0xad});
        final Packet p2 = Packet.createProposal(1, new byte[] {(byte)0xbe,(byte)0xef});

        /**
         * Leader stub
         * @author andre
         *
         */
        new MockUp<Leader>(){
            int times = 0;

            @Mock(invocations = 2)
            public void deliver(byte[] payload){
                ++times;
                switch(times){
                    case 1:
                        assertTrue("We should have received the first package first",
                                Arrays.equals(payload, p1.getPayload()));
                    break;
                    case 2:
                        assertTrue("We should have received the second package second",
                                Arrays.equals(payload, p2.getPayload()));
                    break;
                }
            }

        };

        DeliverStage stage = new DeliverStage(new Leader(null));
        stage.start();
        stage.receiveFromPreviousStage(p1);
        stage.receiveFromPreviousStage(p2);
        Thread.sleep(1000);
        stage.shutdown();
        stage.join();

    }

}

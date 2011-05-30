package br.unicamp.ic.zab.test;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.Test;

import br.unicamp.ic.zab.Packet;
import br.unicamp.ic.zab.PacketSender;


/**
 * This test the packet sender class
 * @author andre
 *
 */
public class PacketSenderTest {


    @Test(timeout=2000)
    public void testEndOfStreamPacket() throws InterruptedException{
     DataOutputStream stream = new DataOutputStream(new ByteArrayOutputStream());
     PacketSender sender = new PacketSender(new Socket(),stream, 1);
     sender.start();
     Thread.sleep(1000);
     assertTrue("SenderThread should be running",sender.isAlive());
     sender.enqueuePacket(Packet.createEndOfStream());
     sender.join();
    }

    @Test(timeout=5000)
    public void testInterruptWhileReceivingPacket() throws InterruptedException{
        DataOutputStream stream = new DataOutputStream(new ByteArrayOutputStream());
        final PacketSender sender = new PacketSender(new Socket(),stream, 1);
        sender.start();
        final AtomicBoolean run = new AtomicBoolean(true);
        Thread sendPackets = new Thread(){
            @Override
            public void run(){
               while(run.get()){
                   try {
                    sender.enqueuePacket(Packet.createPing());
                } catch (InterruptedException e) {
                    fail("InterruptException");
                }
               }
            }
        };
        sendPackets.start();
        Thread.sleep(1000);
        sender.interrupt();
        run.set(false);
        sendPackets.join();
        sender.join();
    }

    @Test(timeout=5000)
    public void testShutdownWithEOSPacket() throws InterruptedException{
        DataOutputStream stream = new DataOutputStream(new ByteArrayOutputStream());
        final PacketSender sender = new PacketSender(new Socket(),stream, 1);
        sender.start();
        final AtomicBoolean run = new AtomicBoolean(true);
        Thread sendPackets = new Thread(){
            @Override
            public void run(){
               while(run.get()){
                   try {
                    sender.enqueuePacket(Packet.createPing());
                } catch (InterruptedException e) {
                    fail("InterruptException");
                }
               }
            }
        };
        sendPackets.start();
        Thread.sleep(1000);
        sender.enqueuePacket(Packet.createEndOfStream());
        run.set(false);
        sendPackets.join();
        sender.join();
    }

    @Test(timeout=5000)
    public void testShutdownWithIOException() throws InterruptedException, IOException{

        class MyOutputStream extends OutputStream{
            private boolean throwIOE = false;

            @Override
            public void write(int arg0) throws IOException {
                if(throwIOE){
                    throw new IOException("Test IOE");
                }

            }

            @Override
            public void close() throws IOException {
                throwIOE = true;
                super.close();
            }


        };
        DataOutputStream stream = new DataOutputStream(new MyOutputStream());

        final PacketSender sender = new PacketSender(new Socket(),stream, 1);
        sender.start();
        final AtomicBoolean run = new AtomicBoolean(true);
        Thread sendPackets = new Thread(){
            @Override
            public void run(){
               while(run.get()){
                   try {
                    sender.enqueuePacket(Packet.createPing());
                } catch (InterruptedException e) {
                    fail("InterruptException");
                }
               }
            }
        };
        sendPackets.start();
        Thread.sleep(1000);
        stream.close();
        run.set(false);
        Thread.sleep(1000);
        sendPackets.join();
        sender.join();
    }

}

package br.unicamp.ic.zab;

import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.log4j.Logger;

/**
 *  Utility class to send packet to leader/follower
 * @author andre
 *
 */
public class PacketSender extends Thread {
    private static final Logger LOG = Logger.getLogger(PacketSender.class);

    /** Thread-safe Queue for packets to be sent to follower*/
    private LinkedBlockingQueue<Packet> outgoingPacketQueue = new LinkedBlockingQueue<Packet>();
    private DataOutputStream outputStream;
    private Socket socket;
    private long destServerId;

    public PacketSender(Socket socket, DataOutputStream output, long destinationServerId){
        super("PacketSender #"+destinationServerId +"@"+ socket.getRemoteSocketAddress());
        this.socket = socket;
        this.outputStream = output;
        this.destServerId = destinationServerId;

    }

    public void enqueuePacket(Packet packet) throws InterruptedException{
        outgoingPacketQueue.put(packet);
    }

    @Override
    public void run() {
        while (true) {
            try {
                Packet packet = outgoingPacketQueue.poll();
                if (packet == null) {
                    // We have not more packet to sent. Flush stream and
                    // wait for next packet
                    outputStream.flush();
                    packet = outgoingPacketQueue.take();
                }
                if (packet.getType() == Packet.Type.END_OF_STREAM) {
                    // Last packet - Finish thread;
                    break;
                }
                LOG.debug("Sending packet to " + destServerId + ": " + packet);
                // Send packet
                packet.toStream(outputStream);
            } catch (InterruptedException e) {
                LOG.warn("Unexpected interruption", e);
                break; // exit thread
            } catch (IOException e) {
                LOG.warn("Some error when sending packets to follower"
                        + destServerId, e);

                if (!socket.isClosed()) {
                    try {
                        // this will cause everything to shutdown on
                        // this learner handler and will help notify
                        // the learner/observer instantaneously
                        socket.close();
                    } catch (IOException ie) {
                        LOG.warn("Some error when closing socket", ie);
                    }
                }
                break;// exit the thread
            }

        }
    }

}

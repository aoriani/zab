package br.unicamp.ic.zab;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.log4j.Logger;

public class FollowerHandler extends Thread {
    private static final Logger LOG = Logger.getLogger(FollowerHandler.class);

    /** Thread-safe Queue for packets to be sent to follower*/
    private LinkedBlockingQueue<Packet> outgoingPacketQueue = new LinkedBlockingQueue<Packet>();
    private Leader leader;
    private Socket socket;
    private long serverId = QuorumPeer.INVALID_SERVER_ID;
    private DataOutputStream toFollowerStream;
    private DataInputStream fromFollowerStream;
    private PacketSender packetSender;

    /**
     *  Utility class to send packet to follower
     * @author andre
     *
     */
    //TODO: Be careful. Using a block queue can block thread
    private class PacketSender extends Thread {

        @Override
        public void run() {
            while (true) {
                try {
                    Packet packet = outgoingPacketQueue.poll();
                    if (packet == null) {
                        // We have not more packet to sent. Flush stream and
                        // wait for next packet
                        toFollowerStream.flush();
                        packet = outgoingPacketQueue.take();
                    }
                    if (packet.getType() == Packet.Type.END_OF_STREAM) {
                        // Last packet - Finish thread;
                        break;
                    }
                    LOG.debug("Sending packet to " + serverId + ": " + packet);
                    // Send packet
                    packet.toStream(toFollowerStream);
                } catch (InterruptedException e) {
                    LOG.warn("Unexpected interruption", e);
                    break; // exit thread
                } catch (IOException e) {
                    LOG.warn("Some error when sending packets to follower"
                            + serverId, e);

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


    public FollowerHandler(Leader leader, Socket followerSocket) {
        super("FollowerHandler " + followerSocket.getRemoteSocketAddress());
        this.leader = leader;
        this.socket = followerSocket;
        leader.addFollowerHandler(this);
    }


    @Override
    public void run() {
        /*
        * Algorithm
        *
        * Read a packet
        * Sanity check for FollowerInfo packet
        * read the server id
        * read the proposal id
        *Lock log
        *     Prepare packets for sync queueing then
        *        See packet  from log that must be sent
        *            Send a proposal and commi for then
        *    Send order to truncate follower log if needed
        *        Enqueue on going proposalt and commits
        *    Add handler to leader list so it can forward packet to the new follower
        *Unlock log
        *Send NewLeaderPAcket
        *Add UPTODATE packet to queue.
        *Fork a thread to send packets;
        *Read next packet - Sanity check for ack - process  ack
        *??Wait until lider is running??
        *
        *while true:
        *    read packet
        *    update tick of handler
        *    handle packet
        *    In case of IOException close socket
        *    finally shutdown
        *
        *
        *shutdown means close socket and remove handler;
        *
        *
        */

        try {
            //Get the Streams
            toFollowerStream = new DataOutputStream(new BufferedOutputStream(socket.getOutputStream()));
            fromFollowerStream = new DataInputStream(new BufferedInputStream(socket.getInputStream()));

            //Read the first packet. It must be a FOLLOWERINFO
            Packet followerInfoPacket = Packet.fromStream(fromFollowerStream);
            if(followerInfoPacket.getType() != Packet.Type.FOLLOWERINFO){
                return; //Finally blocks takes care of clean up
            }
            serverId = followerInfoPacket.getServerId();
            //TODO: handler follower proposalID for sync with leader;

            //Send packet leader
            long newLeaderProposalId = leader.getLastProposalId();
            Packet newLeaderPacket = Packet.createNewLeader(newLeaderProposalId);
            newLeaderPacket.toStream(toFollowerStream);
            toFollowerStream.flush();

            //start send queued packets
            packetSender = new PacketSender();
            packetSender.setName("PacketSender #"+serverId +"@"+ socket.getRemoteSocketAddress());
            packetSender.start();

            handleIncommingPacket();
        } catch (IOException e) {
            if (socket != null && !socket.isClosed()) {
                LOG.error("Unexpected exception causing shutdown while socket still open", e);
                //close the socket to make sure the
                //other side can see it being close
                try {
                    socket.close();
                } catch(IOException ie) {
                    // do nothing
                }
            }

        } finally{
            LOG.warn("Handler for "+serverId+"@"+ (socket!= null?
                    socket.getRemoteSocketAddress():"<unknown>")+"is finishing");
            try {
                outgoingPacketQueue.put(Packet.createEndOfStream());
            } catch (InterruptedException e) {
                LOG.warn("Ignoring unexpected exception", e);
            }
            shutdown();
        }
    }

    private void handleIncommingPacket() throws IOException {
        while(true){
            Packet packet = Packet.fromStream(fromFollowerStream);

            LOG.debug("Received packet from "+serverId+": "+packet);

            switch(packet.getType()){
                case ACKNOWLEDGE:
                    leader.processAcknowledge(serverId,packet.getProposalID());
                break;
                case PING:
                    // Follower responded to ping from leader
                    //This packet only keep socket open and verify liveness
                break;
            }

        }
    }


    /**
    * Queue a packet that will be later sent to follower
    * @param packet the packet to be sent to the follower of this handler
    */
    public void queuePacketToFollower(Packet packet){
        outgoingPacketQueue.add(packet);
    }


    public void shutdown(){
        //TODO: call this from leader to every follower

        LOG.debug("Shuting down follower handler for " + serverId);

        //Free streams and socket
        if(socket!= null && !socket.isClosed()){
            try {
                socket.close();
            } catch (IOException e) {
                LOG.warn("Ignoring exception when closing socket",e);
            }
        }
        interrupt(); //stop the thread
        leader.removeFollowerHandler(this);

    }

}

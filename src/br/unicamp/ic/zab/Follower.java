package br.unicamp.ic.zab;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketException;
import java.util.concurrent.CountDownLatch;

import org.apache.log4j.Logger;

import br.unicamp.ic.zab.stages.DeliverStage;
import br.unicamp.ic.zab.stages.WaitingCommitStage;

public class Follower implements PeerState {
    private static final Logger LOG = Logger.getLogger(Follower.class);

    private QuorumPeer thisPeer;
    private Socket socketToLeader = null;
    private PacketSender packetSender;

    private DataOutputStream toLeaderStream;
    private DataInputStream fromLeaderStream;

    private DeliverStage deliverStage;
    private WaitingCommitStage waitingCommitStage;

    CountDownLatch readyForProposalsLatch = new CountDownLatch(1) ;


    public Follower(QuorumPeer peer){
        thisPeer = peer;
    }

    private QuorumServerSettings discoverLeader(){
        QuorumServerSettings result = null;
        long leaderId = thisPeer.getCurrentVote().id;
        for(QuorumServerSettings qs: thisPeer.getView().values()){
            if(qs.id == leaderId){
                result = qs;
                break;
            }
        }

        if(result == null){
            LOG.warn("Could not find address for leader with id " + leaderId);
        }else{
            LOG.debug("Found leader "+ leaderId + "@" + result.addr.getHostName() +
                    ":" + result.addr.getPort());
        }

        return result;
    }

    private void setupPipeline(){
        deliverStage = new DeliverStage(this);
        deliverStage.start();
        waitingCommitStage = new WaitingCommitStage(deliverStage);
    }

    private Socket connectToLeader(InetSocketAddress address) throws IOException, InterruptedException{
        Socket socket = new Socket();
        final int socketTimeout = thisPeer.getDesirableSocketTimeout();
        try {
            socket.setSoTimeout(socketTimeout);
        } catch (SocketException e) {
            LOG.error("Error while trying to set socket timeout", e);
            throw e;
        }
        InetSocketAddress leaderAddress = address;
        int backoff = Config.getInstance().getFollowerInitialBackoff();
        final int maxConnAttempts = Config.getInstance().getFollowerMaxConnAttempts();

        for(int attempt = 1; attempt <= maxConnAttempts; attempt++){
            try {
                socket.connect(leaderAddress, socketTimeout);
                socket.setTcpNoDelay(Config.getInstance().getTcpDelay());
                break;
            } catch (IOException e) {
                if (attempt == maxConnAttempts){
                    LOG.error("Could not connect to leader", e);
                    throw e;
                }else{
                    LOG.warn("Failed to connect to leader in attempt " +attempt, e);
                }
            }

            //Exponential backoff
            Thread.sleep(backoff);
            backoff *= 2;
        }
        return socket;
    }

    @Override
    public void execute() throws InterruptedException {
        // TODO Auto-generated method stub
        /*
        * Algorithm
        *
        * Discover leader Address
        * Connect with leader - Determine max attempts, socket timeout, tcp delay, user buffered streams
        * Register with Leader
        *     Send the a packet with follower's last proposal id
        *     Read first packet from leader - it must be a NEWLEADER packet
        *     Return leader's proposal id
        * Sanity check leader proposal id => epoch should be higher than follower`s
        * Sync with leader
        * while true
        *     read packet
        *     process packet
        *
        *
        */
        try {
            QuorumServerSettings leaderServer = discoverLeader();
            socketToLeader = connectToLeader(leaderServer.addr);
            toLeaderStream = new DataOutputStream(new BufferedOutputStream(socketToLeader.getOutputStream()));
            fromLeaderStream = new DataInputStream(new BufferedInputStream(socketToLeader.getInputStream()));

            long  newLeaderProposalId = registerWithLeader();
            packetSender = new PacketSender(socketToLeader, toLeaderStream, leaderServer.id);
            packetSender.start();
            setupPipeline();
            //TODO : when sync is implemented we can only ack new leader proposal when we sync
            //Now we are ready send ack for leader proposal
            packetSender.enqueuePacket(Packet.createAcknowledge(newLeaderProposalId));

            //Okay , now we can start proposing
            readyForProposalsLatch.countDown();
            LOG.info("Follower is now ready to send proposals");
            handleIncommingPackets();

        } catch (IOException e) {
            if (socketToLeader != null && !socketToLeader.isClosed()) {
                LOG.error("Unexpected exception causing shutdown while socket still open", e);
                //close the socket to make sure the
                //other side can see it being close
                try {
                    socketToLeader.close();
                } catch(IOException ie) {
                    LOG.warn("Unexpected exception while closing the socket");
                }
            }
        }finally{
            LOG.info("Follower " + thisPeer.getId() + " is finishing");
            shutdown();
        }

    }

    private void handleIncommingPackets() throws IOException, InterruptedException {
        while(true){
            Packet packet = Packet.fromStream(fromLeaderStream);
            LOG.trace("Follower received: " + packet);

            switch(packet.getType()){
                case PROPOSAL:
                    //TODO: when log is implemented we're gonna need a new stage
                    //for now just acknowledge
                    waitingCommitStage.receiveFromPreviousStage(packet);
                    Packet ack = Packet.createAcknowledge(packet.getProposalId());
                    sendPacketToLeader(ack);
                break;

                case PING:
                    //Reply ping
                    Packet ping = Packet.createPing();
                    sendPacketToLeader(ping);
                break;

                case COMMIT:
                    waitingCommitStage.processCommit(packet.getProposalId());
                break;
            }
        }

    }

    private long registerWithLeader() throws IOException{
        //Send Follower Info packet with our last logged proposal id
        long lastLoggedProposalId = thisPeer.getLastLoggedZxid();
        LOG.debug("Telling the leader our last logged proposal was :" + lastLoggedProposalId);
        Packet followerInfo = Packet.createFollowerInfo(lastLoggedProposalId, thisPeer.getId());
        followerInfo.toStream(toLeaderStream);
        toLeaderStream.flush();

        //Read the first packet from leader-It must be a NEWLEADER
        Packet newLeaderPacket = Packet.fromStream(fromLeaderStream);
        if(newLeaderPacket.getType() != Packet.Type.NEWLEADER){
            LOG.error("The first packet sent by leader should be NEWLEADER." +
                        "Got :" + newLeaderPacket);
            throw new IOException("The first packet sent by leader should be NEWLEADER.");
        }
        LOG.trace("Received new leader proposal");

        //Sanity check the epoch of the new leader
        long followerEpoch = (lastLoggedProposalId >> 32L);
        long leaderEpoch = (newLeaderPacket.getProposalId() >> 32L);
        if(leaderEpoch < followerEpoch){
            LOG.error("Leader's epoch " + leaderEpoch + " is smaller than follower's epoch "
                    + followerEpoch);
            throw new IOException("Leader's epoch is smaller than follower's");
        }

        return newLeaderPacket.getProposalId();
    }

    @Override
    public void shutdown() {

        //TODO: do something similar to follower handler for the
        // packet sender
        if(socketToLeader != null && !socketToLeader.isClosed()){
            try {
                socketToLeader.close();
            } catch (IOException e) {
                LOG.warn("Error when closing socket to leader", e);
            }
        }

        try {
            if(packetSender != null){
                //Ensure we finish the sender thread
                packetSender.enqueuePacket(Packet.createEndOfStream());
            }
        } catch (InterruptedException e) {
            LOG.warn("Ignoring unexpected exception", e);
        }

        if(waitingCommitStage != null){
            waitingCommitStage.shutdown();
        }
    }

    @Override
    public synchronized void deliver(byte[] payload) {
        thisPeer.deliver(payload);
    }

    //TODO: should this be synchronized to ensure order in the calls
    public synchronized void sendPacketToLeader(Packet packet) throws InterruptedException{
        if(packetSender != null){
            LOG.debug("Sending packet to leader: " + packet);
            packetSender.enqueuePacket(packet);
        }
    }

    @Override
    public synchronized void propose(byte[] proposal) throws InterruptedException {
        //Wait until follower is ready to send proposal
        readyForProposalsLatch.await();
        Packet request = Packet.createRequest(proposal);
        sendPacketToLeader(request);
    }

}

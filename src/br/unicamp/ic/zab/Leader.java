/**
 *
 */
package br.unicamp.ic.zab;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.log4j.Logger;

/**
 * This implements the leader state
 * Based on Leader
 * @author Andre
 *
 */
public class Leader implements PeerState {
    private static final Logger LOG = Logger.getLogger(Leader.class);



    /**
     * A socket to wait for followers to connect
     */
    private ServerSocket leaderSocket;

    /**
     * A thread to wait for connections and spawn work threads
     * to handle followers
     */
    private FollowerConnectionAcceptor connectionAcceptor;

    /**
     * The peer for this leader
     * Equivalent to self on ZooKeeper
     */
    QuorumPeer thisPeer;

    /**List of handler for connected followers*/
    List<FollowerHandler> followers = new LinkedList<FollowerHandler>();

    /**The proposal id that is increment at each new proposal*/
    AtomicLong proposalId;


    public Leader(QuorumPeer peer) throws IOException{
        thisPeer = peer;
        try {
            leaderSocket = new ServerSocket(thisPeer.getQuorumAddress().getPort());
            LOG.debug("Bound to port "+ thisPeer.getQuorumAddress().getPort());
            proposalId = new AtomicLong(peer.getLastLoggedZxid());
        } catch (IOException e) {
            LOG.error("Error while binding  leader socket", e);
            throw e;
        }
    }



    /* (non-Javadoc)
     * @see br.unicamp.ic.zab.PeerState#execute()
     */
    @Override
    public void execute() {
        /*
         * Algorithm
         * Set tick to zero
         * Load  proposals
         * Increase epoch
         * Create the NEW LEADER proposal with the new epoch
         * Spawn thread to handle peer connecting to this server
         * Add own leader to ack set of NewLEader proposal
         *
         * while new Leader proposal has not quorum
         *     if (tick > initLimit) //follower not syncin' fast
         *         shutdown()
         *         check if enough follow for a quorum
         *             in that case warn to increase initLimit
         *         return
         *     sleep for tick time
         *     increase thick
         *
         *    //ping servers twice a tick
         *  tickSkip = true;
         *  while true
         *      if not tickSkip then increment tick
         *      ping all connected followers
         *      if not tickSkip and do not have quorum of folowers synced
         *          shutdown()
         *          return
         *      invert tickSkip
         *
         *
         */
        connectionAcceptor = new FollowerConnectionAcceptor();
        connectionAcceptor.start();

        //FIXME:Debug
        while(true){
            try {
                Thread.sleep(1000);
                LOG.debug("LEADER DEBUG IDLE LOOP");
            } catch (InterruptedException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();

            }
        }


    }

    /* (non-Javadoc)
     * @see br.unicamp.ic.zab.PeerState#shutdown()
     */
    @Override
    public void shutdown() {

        if(connectionAcceptor != null){
            connectionAcceptor.halt();
        }
        try {
            leaderSocket.close();
        } catch (IOException e) {
            LOG.info("Leader Socket closed with exception",e);
        }


    }


    /**
     * Waits for followers to connect and spawns a thread to handle each of them.
     * Based on Leader.LeanerCnxAcceptor
     * @author Andre
     *
     */
    private class FollowerConnectionAcceptor extends Thread{

        private volatile boolean run = true;

        public FollowerConnectionAcceptor(){
            super("FollowerConnectionAcceptor");
        }

        @Override
        public void run() {
            try {
                while (run){
                    try {
                        Socket followerSocket = leaderSocket.accept();
                        followerSocket.setSoTimeout(thisPeer.getDesirableSocketTimeout());
                        followerSocket.setTcpNoDelay(Settings.TCP_NODELAY);
                        FollowerHandler handler = new FollowerHandler(Leader.this,followerSocket);
                        handler.start();
                    } catch (SocketException e) {
                        if(!run){
                            //When we shutdown the leader we close the socket
                            // so a exception is expected
                            LOG.info("Exception while shuting down the leader",e);
                        }else{
                            throw e;
                        }
                    }
                }
            } catch (Exception e) {
                LOG.warn("Error while accepting Followers connections",e);
            }
        }

        public void halt(){
            run = false;
            //TODO: consider if we should wait this thread to terminate
        }
    }


    public long getLastProposalId() {
        long currentProposalId = proposalId.get();
        return currentProposalId;
    }

    public long getNextProposalId(){
        //TODO: How to control the number of inFlightPackets;
        long nextProposalId = proposalId.incrementAndGet();
        return nextProposalId;
    }



    public void processAcknowledge(long serverId, long proposalID) {
        //Check it it is for the leader proposal, if not let the stage to
        //take care

    }


    /**
     * Adds handler to follower from leader's list
     * @param followerHandler the handler to be added
     */
    public void addFollowerHandler(FollowerHandler followerHandler) {
        synchronized(followers){
            followers.add(followerHandler);
        }
    }

    /**
     * Removes handler to follower from leader's list
     * @param followerHandler the handler to be removed
     */
    public void removeFollowerHandler(FollowerHandler followerHandler) {
        synchronized(followers){
            followers.remove(followerHandler);
        }
    }



    /**
     * Send a packet to all follower
     * @param packet the packet to be sent
     */
    public void sendPacketToFollowers(Packet packet) {
        //FIXME: Only send to synced followers
        synchronized(followers){
            for(FollowerHandler handler:followers){
                handler.queuePacketToFollower(packet);
            }
        }

    }

    /**
    * Get the server id for leader
    * @return the id of leader in the quorum
    */
    public Long getId() {
        return thisPeer.getId();
    }

    /**
    * Get the quorum verifier used by this peer
    * @return a quorum verifier object
    */
    public QuorumVerifier getQuorumVerifier() {
        return thisPeer.getQuorumVerifier();
    }



    public void deliver(byte[] payload) {
        // TODO Auto-generated method stub

    }







}

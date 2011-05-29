/**
 *
 */
package br.unicamp.ic.zab;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.log4j.Logger;

import br.unicamp.ic.zab.stages.DeliverStage;
import br.unicamp.ic.zab.stages.ProposalStage;
import br.unicamp.ic.zab.stages.SendCommitStage;
import br.unicamp.ic.zab.stages.WaitingAckStage;

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
    AtomicLong currentProposalId;

    private int tick;



    private long newLeaderProposalId;
    private HashSet<Long> newLeaderProposalAckSet;


    //The Pipeline
    ProposalStage proposalStage;
    WaitingAckStage waitingAckStage;
    SendCommitStage sendCommitStage;
    DeliverStage deliverStage;

    CountDownLatch readyForProposalsLatch = new CountDownLatch(1) ;



    public Leader(QuorumPeer peer) throws IOException{
        thisPeer = peer;
        try {
            leaderSocket = new ServerSocket(thisPeer.getQuorumAddress().getPort());
            LOG.debug("Bound to port "+ thisPeer.getQuorumAddress().getPort());
            currentProposalId = new AtomicLong(peer.getLastLoggedZxid());
        } catch (IOException e) {
            LOG.error("Error while binding  leader socket", e);
            throw e;
        }
    }


    private void setupPipeline(){
        deliverStage = new DeliverStage(this);
        deliverStage.start();
        sendCommitStage = new SendCommitStage(this, deliverStage);
        sendCommitStage.start();
        waitingAckStage = new WaitingAckStage(this, sendCommitStage);
        proposalStage = new ProposalStage(this, waitingAckStage);
        proposalStage.start();

    }


    /* (non-Javadoc)
     * @see br.unicamp.ic.zab.PeerState#execute()
     */
    @Override
    public void execute() throws InterruptedException {
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

        final int tickTime = thisPeer.getTickTime();
        tick = 0;

        //set a new epoch
        long previousEpoch = thisPeer.getLastLoggedZxid() >> 32L;//Bring 4 higher bytes to lower bytes
        long nextEpoch = (++previousEpoch) << 32L; //Increment and bring back
        currentProposalId.set(nextEpoch);

        //Create the new leader
        newLeaderProposalId = getLastProposalId();
        newLeaderProposalAckSet = new HashSet<Long>();
        newLeaderProposalAckSet.add(getId()); //Add the leader to its quorum

        //setPipeline
        setupPipeline();

        //Create the thread to wait for the followers
        connectionAcceptor = new FollowerConnectionAcceptor();
        connectionAcceptor.start();

        // We have to get at least a majority of servers in sync with
        // us. We do this by waiting for the NEWLEADER packet to get
        // acknowledged
        while (!thisPeer.getQuorumVerifier().containsQuorum(
                newLeaderProposalAckSet)) {
            if (tick > thisPeer.getSyncLimit()) {
                // Follower are not syncing fast enough
                // renounce leadership
                if (LOG.isInfoEnabled()) {
                    StringBuilder builder = new StringBuilder();
                    builder.append("Only synced with { ");
                    for (Long server : newLeaderProposalAckSet) {
                        builder.append(server).append(" ");
                    }
                    builder.append("} . So no quorum, renouncing leadership");
                }
                shutdown();

                HashSet<Long> followerSet = new HashSet<Long>();
                for (FollowerHandler f : followers)
                    followerSet.add(f.getId());

                if (this.getQuorumVerifier().containsQuorum(followerSet)) {
                    // if (followers.size() >= self.quorumPeers.size() / 2) {
                    LOG.warn("Enough followers present. "
                            + "Perhaps the initTicks need to be increased.");
                }
                //Exit the leader state
                return;
            }
            Thread.sleep(tickTime);
            tick++;
        }

        //Now that we have quorum, we can propose
        readyForProposalsLatch.countDown();
        LOG.info("Leader read to receive proposals");

        // We ping twice a tick, so we only update the tick every other
        // iteration
        boolean tickSkip = true;

        while (true) {
            Thread.sleep(tickTime / 2);
            if (!tickSkip) {
                tick++;
            }
            int syncedCount = 0;
            HashSet<Long> syncedSet = new HashSet<Long>();

            // lock on the followers when we use it.
            syncedSet.add(getId());
            synchronized (followers) {
                for (FollowerHandler f : followers) {
                    if (f.synced()) {
                        syncedCount++;
                        syncedSet.add(f.getServerId());
                    }
                    f.ping();
                }
            }
          if (!tickSkip && !thisPeer.getQuorumVerifier().containsQuorum(syncedSet)) {
            //if (!tickSkip && syncedCount < self.quorumPeers.size() / 2) {
                // Lost quorum, shutdown
              // TODO: message is wrong unless majority quorums used
                LOG.info("Only " + syncedCount + " followers, need "
                        + (thisPeer.getView().size() / 2));
                // make sure the order is the same!
                // the leader goes to looking
                shutdown();
                return;
          }
          tickSkip = !tickSkip;
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

        if(proposalStage != null){
            proposalStage.shutdown();
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
        long proposalId = currentProposalId.get();
        return proposalId;
    }

    public long getNextProposalId(){
        //TODO: How to control the number of inFlightPackets;
        long proposalId = currentProposalId.incrementAndGet();
        return proposalId;
    }



    public synchronized void processAcknowledge(long serverId, long proposalId)
        throws InterruptedException {
        //Check if it is for the new leader proposal
        //if not , let the right stage to take care
        if(proposalId == newLeaderProposalId){
            synchronized(newLeaderProposalAckSet){
                newLeaderProposalAckSet.add(serverId);
            }
        }else{
            waitingAckStage.processAck(serverId,proposalId);
        }

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
     * @throws InterruptedException
     */
    public void sendPacketToFollowers(Packet packet) throws InterruptedException {
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

    @Override
    public synchronized void deliver(byte[] payload) {
        thisPeer.deliver(payload);
    }

    public long getNewLeaderProposalId() {
        return newLeaderProposalId;
    }

    //synchronized to ensure packet are queue in the same order proposal id
    // is generated , so packets in queue are always ordered
    @Override
    public synchronized void propose(byte[] data) throws InterruptedException{

        //Wait until leader is ready to send proposal
        readyForProposalsLatch.await();

        long proposalId = getNextProposalId();
        Packet proposal = Packet.createProposal(proposalId, data);
        proposalStage.receiveFromPreviousStage(proposal);
    }


    public long getTick(){
        return tick;
    }

    public long getSyncLimit(){
        return thisPeer.getSyncLimit();
    }

}

/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package br.unicamp.ic.zab;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.Map;

import org.apache.log4j.Logger;

/**
 * This class manages the quorum protocol. There are three states this server
 * can be in:
 * <ol>
 * <li>Leader election - each server will elect a leader (proposing itself as a
 * leader initially).</li>
 * <li>Follower - the server will synchronize with the leader and replicate any
 * transactions.</li>
 * <li>Leader - the server will process requests and forward them to followers.
 * A majority of followers must log the request before it can be accepted.
 * </ol>
 */

public class QuorumPeer extends Thread {
    public static class QuorumServer {
        public InetSocketAddress addr;

        public InetSocketAddress electionAddr;

        public long id;

        public QuorumServer(long id, InetSocketAddress addr) {
            this.id = id;
            this.addr = addr;
            this.electionAddr = null;
        }

        public QuorumServer(long id, InetSocketAddress addr,
                InetSocketAddress electionAddr) {
            this.id = id;
            this.addr = addr;
            this.electionAddr = electionAddr;
        }

    }

    public enum ServerState {
        LOOKING, FOLLOWING, LEADING
    }

    private static final Logger LOG = Logger.getLogger(QuorumPeer.class);

    /**
    * This is who I think the leader currently is.
    */
    volatile private Vote currentVote;

    /**
    * The algorithm for election
    */
    private FastLeaderElection electionAlg;
    /**
    * The number of ticks that the initial synchronization phase can take
    */
    protected int initLimit;

    /**
    * The id of this server
    */
    private long myid;

    /**
    * The port for election of this server
    */
    private InetSocketAddress myQuorumAddr;

    /**
    * Controls the connections of this peer with all other peers
    */

    QuorumCnxManager qcm;

    /**
    * QuorumVerifier implementation.
    */

    private QuorumVerifier quorumConfig;

    /**
    * The servers that make up the cluster
    */
    protected Map<Long, QuorumServer> quorumPeers;

    volatile boolean running = true;

    private ServerState state = ServerState.LOOKING;

    PeerState currentState = null;

    /**
    * The number of ticks that can pass between sending a request and getting
    * an acknowledgment
    */
    protected int syncLimit;

    /**
    * The current tick
    */
    protected int tick;

    /**
    * The number of milliseconds of each tick
    */
    protected int tickTime;

    public QuorumPeer(Map<Long, QuorumServer> quorumPeers, long myid,
            int tickTime, int initLimit, int syncLimit) throws IOException {
        super("QuorumPeer");
        this.quorumPeers = quorumPeers;
        this.myid = myid;
        this.tickTime = tickTime;
        this.initLimit = initLimit;
        this.syncLimit = syncLimit;
        this.quorumConfig = new QuorumVerifier(quorumPeers.size());
    }

    protected FastLeaderElection createElectionAlgorithm() {
        qcm = new QuorumCnxManager(this);
        QuorumCnxManager.Listener listener = qcm.listener;
        if (listener != null) {
            listener.start();
            return new FastLeaderElection(this, qcm);
        } else {
            return null;
        }

    }

    public synchronized Vote getCurrentVote() {
        return currentVote;
    }

    /**
    * get the id of this quorum peer.
    */
    public long getId() {
        return myid;
    }

    /**
    * Get the number of ticks that the initial synchronization phase can take
    */
    public int getInitLimit() {
        return initLimit;
    }

    /**
    * returns the highest zxid that this host has seen
    *
    * @return the highest zxid for this host
    */
    public long getLastLoggedZxid() {
        // TODO: return value from log
        return 0;
    }

    /**
    * get the id of this quorum peer.
    */
    public long getMyid() {
        return myid;
    }

    public synchronized ServerState getPeerState() {
        return state;
    }

    public InetSocketAddress getQuorumAddress() {
        return myQuorumAddr;
    }

    /**
    * get reference to QuorumCnxManager
    */
    public QuorumCnxManager getQuorumCnxManager() {
        return qcm;
    }

    public int getQuorumSize() {
        return getView().size();
    }

    /**
    * Return QuorumVerifier object
    */

    public QuorumVerifier getQuorumVerifier() {
        return quorumConfig;

    }

    /**
    * Get the synclimit
    */
    public int getSyncLimit() {
        return syncLimit;
    }

    /**
    * Get the current tick
    */
    public int getTick() {
        return tick;
    }

    /**
    * Get the number of milliseconds of each tick
    */
    public int getTickTime() {
        return tickTime;
    }

    /**
    * A 'view' is a node's current opinion of the membership of the entire
    * ensemble.
    */
    public Map<Long, QuorumPeer.QuorumServer> getView() {
        return Collections.unmodifiableMap(this.quorumPeers);
    }

    public boolean isRunning() {
        return running;
    }

    @Override
    public void run() {
        setName("QuorumPeer" + "[myid=" + getId() + "]");

        LOG.debug("Starting quorum peer");

        try {
            /*
            * Main loop
            */
            while (running) {
                switch (getPeerState()) {
                case LOOKING:
                    try {
                        LOG.info("LOOKING");
                        setCurrentVote(electionAlg.lookForLeader());
                    } catch (Exception e) {
                        LOG.warn("Unexpected exception", e);
                        setPeerState(ServerState.LOOKING);
                    }
                    break;
                case FOLLOWING:
                    try {
                        LOG.info("FOLLOWING");
                        currentState = new Follower(this);
                        currentState.execute();
                    } catch (Exception e) {
                        LOG.warn("Unexpected exception", e);
                    } finally {
                        currentState.shutdown();
                        currentState = null;
                        setPeerState(ServerState.LOOKING);
                    }
                    break;
                case LEADING:
                    LOG.info("LEADING");
                    try {
                        currentState = new Leader(this);
                        currentState.execute();
                    } catch (Exception e) {
                        LOG.warn("Unexpected exception", e);
                    } finally {
                        currentState.shutdown();
                        currentState = null;
                        setPeerState(ServerState.LOOKING);
                    }
                    break;
                }
            }
        } finally {

        }
    }

    public synchronized void setCurrentVote(Vote v) {
        currentVote = v;
    }

    /**
    * Set the number of ticks that the initial synchronization phase can take
    */
    public void setInitLimit(int initLimit) {
        LOG.info("initLimit set to " + initLimit);
        this.initLimit = initLimit;
    }

    /**
    * set the id of this quorum peer.
    */
    public void setMyid(long myid) {
        this.myid = myid;
    }

    public synchronized void setPeerState(ServerState newState) {
        state = newState;
    }

    public void setQuorumPeers(Map<Long, QuorumServer> quorumPeers) {
        this.quorumPeers = quorumPeers;
    }

    public void setQuorumVerifier(QuorumVerifier quorumConfig) {
        this.quorumConfig = quorumConfig;
    }

    public void setRunning(boolean running) {
        this.running = running;
    }

    /**
    * Set the synclimit
    */
    public void setSyncLimit(int syncLimit) {
        this.syncLimit = syncLimit;
    }

    /**
    * Set the number of milliseconds of each tick
    */
    public void setTickTime(int tickTime) {
        LOG.info("tickTime set to " + tickTime);
        this.tickTime = tickTime;
    }

    public void shutdown() {
        //TODO: Verify what should be shut down here
        running = false;
        if (electionAlg != null) {
            electionAlg.shutdown();
            electionAlg = null;
        }
        if(currentState != null){
            currentState.shutdown();
            currentState = null;
        }

    }

    @Override
    public synchronized void start() {
        startLeaderElection();
        super.start();
    }

    public synchronized void startLeaderElection() {
        currentVote = new Vote(myid, getLastLoggedZxid());
        for (QuorumServer p : getView().values()) {
            if (p.id == myid) {
                myQuorumAddr = p.addr;
                break;
            }
        }
        if (myQuorumAddr == null) {
            throw new RuntimeException("My id " + myid
                    + " not in the peer list");
        }

        electionAlg = createElectionAlgorithm();
    }

    /**
    * Check if a node is in the current view. With static membership, the
    * result of this check will never change; only when dynamic membership is
    * introduced will this be more useful.
    */
    public boolean viewContains(Long sid) {
        return this.quorumPeers.containsKey(sid);
    }

    public int getDesirableSocketTimeout() {
        return getTickTime() * getSyncLimit();
    }
}

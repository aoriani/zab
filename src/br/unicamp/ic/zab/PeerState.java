package br.unicamp.ic.zab;

/**
 *
 * Interfaces for classes implementing
 * the peer states listed on br.unicamp.ic.zab.QuorumPeerServerState
 *
 */
public interface PeerState {

//TODO Bring Enum to here

    /**
    * The main method for a state
    * @throws InterruptedException
    */
    public void execute() throws InterruptedException;

    /**
    * This method must releases all resources
    * and stop all threads since peer is leaving
    * the state
    *
    */
    public void shutdown();

    /**
     * This method is called when the protocol has
     * data to be delivered to the peer
     * @param payload data to be delivered
     */
    public void deliver(byte[] payload);


    /**
     * This send a proposal
     * In the case of leader it send a proposal to all follower
     * In the case of follower , it redirects to leader, so that one can propose
     * @param proposal
     * @throws InterruptedException
     */
    public void propose(byte[] proposal) throws InterruptedException;

}

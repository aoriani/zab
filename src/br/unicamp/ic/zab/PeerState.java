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

}

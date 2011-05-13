package br.unicamp.ic.zab;

public class FollowerHandler extends Thread {

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

    }




}

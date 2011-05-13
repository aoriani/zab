package br.unicamp.ic.zab;

/**
 * A class to store settings
 *
 * @author Andre
 *
 */
//TODO: Refine this class
public class Settings {

    /** Whether disable Nagle's Algorithm for TCP, improving latency for small packets */
    public static final boolean TCP_NODELAY = true;
    /** How many times follower must try to connect to leader before giving up */
    public static final int FOLLOWER_MAX_ATTEMPS_CONNECT = 5;
    /** Initial time for the exponential backoff of follower connecting to leader */
    public static final int FOLLOWER_INITIAL_BACKOFF = 500;

}

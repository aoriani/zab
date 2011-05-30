package br.unicamp.ic.zab;

/**
 * A callback to notify clients of Zab
 * of important events
 * @author Andre
 *
 */
public interface Callback {

    /**
     * This callback is called to deliver data comming from the broadcast.
     * To ensure good performance, this callback must return ASAP.
     * @param payload the data to be delivered
     */
    void deliver(byte[] payload);

}

package br.unicamp.ic.zab;

import java.net.InetSocketAddress;

public class QuorumServerSettings {
    public InetSocketAddress addr;

    public InetSocketAddress electionAddr;

    public long id;

    public QuorumServerSettings(long id, InetSocketAddress addr,
            InetSocketAddress electionAddr) {
        this.id = id;
        this.addr = addr;
        this.electionAddr = electionAddr;
    }

}

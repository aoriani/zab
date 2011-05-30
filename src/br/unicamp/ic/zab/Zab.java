package br.unicamp.ic.zab;

import java.util.Map;

public class Zab {



    private QuorumPeer thisPeer;

    public Zab (long myId, Callback callback){
        final Config config = Config.getInstance();
        final int tickTime = config.getTickTime();
        final int initLimit = config.getInitLimit();
        final int syncLimit = config.getSyncLimit();
        final Map<Long,QuorumServerSettings> quorumServers =
                config.getQuorumServers();
        thisPeer = new QuorumPeer(quorumServers, myId, tickTime, initLimit, syncLimit, callback);

    }


    public void start(){
        thisPeer.start();
    }

    public boolean propose(byte[] proposal) throws InterruptedException{
       return thisPeer.propose(proposal);
    }


    public void shutdown(){
        thisPeer.shutdown();
    }


}

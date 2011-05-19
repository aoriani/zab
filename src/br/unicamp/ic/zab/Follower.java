package br.unicamp.ic.zab;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketException;

import org.apache.log4j.Logger;

import br.unicamp.ic.zab.QuorumPeer.QuorumServer;

public class Follower implements PeerState {
    private static final Logger LOG = Logger.getLogger(Follower.class);

    private QuorumPeer thisPeer;
    private Socket socketToLeader = null;

    public Follower(QuorumPeer peer){
        thisPeer = peer;
    }

    private InetSocketAddress discoverLeaderAddress(){
        InetSocketAddress result = null;
        long leaderId = thisPeer.getCurrentVote().id;
        for(QuorumServer qs: thisPeer.getView().values()){
            if(qs.id == leaderId){
                result = qs.addr;
                break;
            }
        }

        if(result == null){
            LOG.warn("Could not find address for leader with id " + leaderId);
        }else{
            LOG.debug("Found leader "+ leaderId + "@" + result.getHostName() + ":" + result.getPort());
        }

        return result;
    }

    private Socket connectToLeader() throws IOException, InterruptedException{
        Socket socket = new Socket();
        final int socketTimeout = thisPeer.getDesirableSocketTimeout();
        try {
            socket.setSoTimeout(socketTimeout);
        } catch (SocketException e) {
            LOG.error("Error while trying to set socket timeout", e);
            throw e;
        }
        InetSocketAddress leaderAddress =  discoverLeaderAddress();
        int backoff = Settings.FOLLOWER_INITIAL_BACKOFF;

        for(int attempt = 1; attempt <= Settings.FOLLOWER_MAX_ATTEMPS_CONNECT; attempt++){
            try {
                socket.connect(leaderAddress, socketTimeout);
                socket.setTcpNoDelay(Settings.TCP_NODELAY);
                break;
            } catch (IOException e) {
                if (attempt == Settings.FOLLOWER_MAX_ATTEMPS_CONNECT){
                    LOG.error("Could not connect to leader", e);
                    throw e;
                }else{
                    LOG.warn("Failed to connect to leader in attempt " +attempt, e);
                }
            }

            //Exponential backoff
            Thread.sleep(backoff);
            backoff *= 2;
        }
        return socket;
    }

    @Override
    public void execute() throws InterruptedException {
        // TODO Auto-generated method stub
        /*
        * Algorithm
        *
        * Discover leader Address
        * Connect with leader - Determine max attempts, socket timeout, tcp delay, user buffered streams
        * Register with Leader
        *     Send the a packet with follower's last proposal id
        *     Read first packet from leader - it must be a NEWLEADER packet
        *     Return leader's proposal id
        * Sanity check leader proposal id => epoch should be higher than follower`s
        * Sync with leader
        * while true
        *     read packet
        *     process packet
        *
        *
        */
        try {
            socketToLeader = connectToLeader();

            //FIXME:Debug
            while(true){
                Thread.sleep(1000);
                LOG.debug("FOLLOWER DEBUG IDLE LOOP");
            }

        } catch (IOException e) {

        }finally{
            try {
                socketToLeader.close();
                socketToLeader = null;
            } catch (IOException e) {
                LOG.warn("Error when closing socket to leader", e);
            }
        }

    }

    @Override
    public void shutdown() {
        if(socketToLeader != null){
            try {
                socketToLeader.close();
            } catch (IOException e) {
                LOG.warn("Error when closing socket to leader", e);
            }
        }
    }

}

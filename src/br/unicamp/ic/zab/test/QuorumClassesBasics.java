package br.unicamp.ic.zab.test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.HashMap;

import org.apache.log4j.Logger;

import br.unicamp.ic.zab.QuorumPeer;
import br.unicamp.ic.zab.QuorumPeer.QuorumServer;

public class QuorumClassesBasics {

    private static final Logger LOG = Logger.getLogger(QuorumClassesBasics.class);


	/**
	 * @param args
	 */
	public static void main(String[] args) {

		Long myid = Long.parseLong(args[0]);

		QuorumServer s1 = new QuorumServer(1l,new InetSocketAddress("127.0.0.1",3331),new InetSocketAddress("127.0.0.1",4441));
		QuorumServer s2 = new QuorumServer(2l,new InetSocketAddress("127.0.0.1",3332),new InetSocketAddress("127.0.0.1",4442));
		QuorumServer s3 = new QuorumServer(3l,new InetSocketAddress("127.0.0.1",3333),new InetSocketAddress("127.0.0.1",4443));

		HashMap<Long,QuorumServer> quorumPeers = new HashMap<Long,QuorumServer> ();
		quorumPeers.put(1l, s1);
		quorumPeers.put(2l, s2);
		quorumPeers.put(3l, s3);

		try {
			QuorumPeer peer = new QuorumPeer(quorumPeers, myid, 2000, 10, 5);
			peer.start();
			peer.join();
		} catch (IOException e) {
			LOG.error("Some problem when starting peer", e);
		} catch (InterruptedException e) {
			LOG.warn("Peer has been interrupted", e);
		}


	}

}

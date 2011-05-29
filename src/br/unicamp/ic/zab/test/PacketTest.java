package br.unicamp.ic.zab.test;

import static org.junit.Assert.assertEquals;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import org.junit.Test;

import br.unicamp.ic.zab.Packet;

/**
 * Unit test for Packet class
 *
 * @author Andre
 *
 */
public class PacketTest {

    /**
    * Helper function to test a serialization of packet It takes a sample
    * packet , serializes it, rebuilt the packet and verifies if the rebuilt
    * packet is equals to the original
    *
    * @param packet
    *            the sample packet for the test
    * @throws IOException
    */
    private void testSerializationHelper(Packet packet) throws IOException {
        ByteArrayOutputStream bout = new ByteArrayOutputStream();
        packet.toStream(new DataOutputStream(bout));
        ByteArrayInputStream bin = new ByteArrayInputStream(bout.toByteArray());
        Packet packet2 = Packet.fromStream(new DataInputStream(bin));
        assertEquals("Input packet and output packet should be the same",
                packet, packet2);
    }

    /**
    * Tests if a proposal packet is created, serialized and rebuilt correctly
    *
    * @throws IOException
    */
    @Test
    public void testProposalSerialization() throws IOException {
        byte[] payload = { (byte) 0xde, (byte) 0xad, (byte) 0xbe, (byte) 0xef };
        Packet packet = Packet.createProposal(0xdeadbeef, payload);
        testSerializationHelper(packet);
    }

    /**
    * Tests if a ping packet is created, serialized and rebuilt correctly
    *
    * @throws IOException
    */
    @Test
    public void testPingSerialization() throws IOException {
        Packet packet = Packet.createPing();
        testSerializationHelper(packet);
    }


    /**
     * Tests if a end of stream packet is created, serialized and rebuilt correctly
     *
     * @throws IOException
     */
     @Test
     public void testEndOfStreamSerialization() throws IOException {
         Packet packet = Packet.createEndOfStream();
         testSerializationHelper(packet);
     }

    /**
    * Tests if a new leader packet is created, serialized and rebuilt correctly
    *
    * @throws IOException
    */
    @Test
    public void testNewLeaderSerialization() throws IOException {
        Packet packet = Packet.createNewLeader(0xdeadbeef);
        testSerializationHelper(packet);
    }

    /**
    * Tests if a acknowledge packet is created, serialized and rebuilt
    * correctly
    *
    * @throws IOException
    */
    @Test
    public void testAcknowlegdeSerialization() throws IOException {
        Packet packet = Packet.createAcknowledge(0xdeadbeef);
        testSerializationHelper(packet);
    }

    /**
    * Tests if a commit packet is created, serialized and rebuilt correctly
    *
    * @throws IOException
    */
    @Test
    public void testCommitSerialization() throws IOException {
        Packet packet = Packet.createCommit(0xdeadbeef);
        testSerializationHelper(packet);
    }

    /**
    * Tests if a follower info packet is created, serialized and rebuilt
    * correctly
    *
    * @throws IOException
    */
    @Test
    public void testFollowerInfoSerialization() throws IOException {
        Packet packet = Packet.createFollowerInfo(0xdeadbeef, 5);
        testSerializationHelper(packet);
    }

    @Test
    public void testRequestSerialization() throws IOException {
        byte[] payload = { (byte) 0xde, (byte) 0xad, (byte) 0xbe, (byte) 0xef };
        Packet packet = Packet.createRequest(payload);
        testSerializationHelper(packet);
    }

    /**
    * Tests if for a follower info packet, the server id is stored in payload
    * correctly
    */
    @Test
    public void testFollowerInfoGetServerId() {
        Packet packet = Packet.createFollowerInfo(0xdeadbeef, 5);
        assertEquals("The expected server id for this packet was five", 5,
                packet.getServerId());
    }

    /**
    * Tests if for a non follower info packet, getServerId throws UnsupportedOperationException
    */
    @Test(expected = UnsupportedOperationException.class)
    public void testFollowerInfoGetServerIdUnsupportOperationWrongType() {
        Packet.createPing().getServerId();
    }

}

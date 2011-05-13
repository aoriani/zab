package br.unicamp.ic.zab;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;

import org.apache.log4j.Logger;

/**
 * The atomic unit of the leader-followe protocol
 * @author Andre
 *
 */

//TODO: Possible refactoring : Packet factory class, subclasses implement specific marshalling
//TODO: Perhaps we need to implement equals
public class Packet {
    private static final Logger LOG = Logger.getLogger(Packet.class);

    /**
     * The type of packet
     * @author Andre
     *
     */
    public enum Type{
        /**Packet sent to all follower to claim leadership */
        NEWLEADER,
        /**Packet sent by leader to propose something */
        PROPOSAL,
        /**Packet sent by follower to acknowledge proposal*/
        ACKNOWLEDGE,
        /**Packet sent by leader to tell followers a proposal can be committed*/
        COMMIT,
        /** Leader and Followers exchange ping to check for node liveness*/
        PING,
        /**When follower connects to leader, follower send its current state to leader */
        FOLLOWERINFO;

        /**
         * For serialization purposes, convert an int to Type
         * @param i an int represent a packet type
         * @return the respective item of the Type enum
         */
        public static Type valueOf(int i){
            switch(i){
                case 0:
                    return NEWLEADER;
                case 1:
                    return PROPOSAL;
                case 2:
                    return ACKNOWLEDGE;
                case 3:
                    return COMMIT;
                case 4:
                    return PING;
                case 5:
                    return FOLLOWERINFO;
                default:
                    throw new IllegalArgumentException("" + i + " does not match a packet type");
            }
        }

        /**
         * For serialization purposes convert Type to int
         * @return an int representing  the respective Type enum item
         */
        public int value(){
            switch(this){
                case NEWLEADER:
                    return 0;
                case PROPOSAL:
                    return 1;
                case ACKNOWLEDGE:
                    return 2;
                case COMMIT:
                    return 3;
                case PING:
                    return 4;
                case FOLLOWERINFO:
                    return 5;
                default:
                    throw new IllegalStateException("Forget to implement value() for " + this);
            }
        }

    }

    public static final long INVALID_PROPOSAL_ID = -1;

    private Type type;
    private long proposalID;
    private byte[] payload;


    private Packet() {}

    public Type getType(){
        return type;
    }

    public long getProposalID(){
        return proposalID;
    }

    public byte[] getPayload(){
        //TODO: Should I return a copy for safety?FindBugs agree, but performance?
        return payload;
    }


    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + Arrays.hashCode(payload);
        result = prime * result + (int) (proposalID ^ (proposalID >>> 32));
        result = prime * result + ((type == null) ? 0 : type.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        Packet other = (Packet) obj;
        if (!Arrays.equals(payload, other.payload))
            return false;
        if (proposalID != other.proposalID)
            return false;
        if (type != other.type)
            return false;
        return true;
    }

    @Override
    public String toString() {
        StringBuffer repr = new StringBuffer();
        repr.append("Packet@"+ hashCode()+" {type:" + type.toString());
        repr.append(", proposalID:");
        if(proposalID != INVALID_PROPOSAL_ID){
            repr.append(proposalID);
        }else{
            repr.append("<invalid>");
        }
        repr.append(", payload:");
        if(payload != null){
            repr.append(payload.length).append(" bytes}");
        }else{
            repr.append("<empty> }");
        }

        return repr.toString();
    }



    /**
     * De-serialize a packet from a stream
     * @param in the input stream
     * @return the packet inflated from stream
     * @throws IOException
     */
    public static Packet fromStream(DataInputStream in) throws IOException{
        Packet packet = new Packet();
        packet.type = Type.valueOf(in.readInt());

        switch(packet.type){
            case NEWLEADER:
            case ACKNOWLEDGE:
            case COMMIT:
                packet.proposalID = in.readLong();
                packet.payload = null;
            break;

            case PING:
                packet.proposalID = INVALID_PROPOSAL_ID;
                packet.payload = null;
            break;

            case PROPOSAL:
            case FOLLOWERINFO:
                packet.proposalID = in.readLong();
                int payloadSize = in.readInt();
                packet.payload = new byte[payloadSize];
                in.readFully(packet.payload);
            break;

            default:
                throw new IllegalArgumentException("Unexpected packet type");
        }
        LOG.trace(packet);
        return packet;
    }


    /**
     * Marshals a packet into a stream
     * @param out the output to which the packet should be serialized
     * @throws IOException
     */
    public void toStream(DataOutputStream out) throws IOException{
        out.writeInt(type.value());

        switch(type){
            case NEWLEADER:
            case PROPOSAL:
            case ACKNOWLEDGE:
            case COMMIT:
            case FOLLOWERINFO:
                out.writeLong(proposalID);
            break;
        }

        if(type == Type.PROPOSAL || type == Type.FOLLOWERINFO){
            out.writeInt(payload.length);
            out.write(payload);
        }
    }


    public static Packet createProposal(long proposalId, byte[] buffer){
        Packet packet  = new Packet();
        packet.type = Type.PROPOSAL;
        packet.proposalID = proposalId;
        packet.payload = buffer; // Should I copy ?
        return packet;
    }

    public static Packet createNewLeader(long proposalId){
        Packet packet  = new Packet();
        packet.type = Type.NEWLEADER;
        packet.proposalID = proposalId;
        packet.payload = null;
        return packet;
    }

    public static Packet createAcknowledge(long proposalId){
        Packet packet  = new Packet();
        packet.type = Type.ACKNOWLEDGE;
        packet.proposalID = proposalId;
        packet.payload = null;
        return packet;
    }

    public static Packet createCommit(long proposalId){
        Packet packet  = new Packet();
        packet.type = Type.COMMIT;
        packet.proposalID = proposalId;
        packet.payload = null;
        return packet;
    }

    public static Packet createPing(){
        Packet packet  = new Packet();
        packet.type = Type.PING;
        packet.proposalID = INVALID_PROPOSAL_ID;
        packet.payload = null;
        return packet;
    }

    public static Packet createFollowerInfo(long proposalId, long serverId){
        Packet packet = new Packet();
        packet.type = Type.FOLLOWERINFO;
        packet.proposalID = proposalId;
        final int SIZEOF_LONG = 8;
        packet.payload = new byte[SIZEOF_LONG];
        ByteBuffer buffer = ByteBuffer.wrap(packet.payload);
        buffer.putLong(serverId);
        return packet;
    }

    public long getServerId(){
        final int SIZEOF_LONG = 8;
        if(type != Type.FOLLOWERINFO || payload == null || payload.length != SIZEOF_LONG){
            throw new UnsupportedOperationException("Cannot get server id for a non"+
                    "FOLLOWERINFO packet or a packet with incompatible payload");
        }
        ByteBuffer buffer = ByteBuffer.wrap(payload);
        return buffer.getLong();
    }

}

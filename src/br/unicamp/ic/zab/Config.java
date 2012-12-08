package br.unicamp.ic.zab;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;

import org.apache.log4j.Logger;

/**
 * This holds the configuration data of the protocol
 *
 * @author Andre
 *
 */
//TODO: Refine this class
public class Config {
    private static final Logger LOG = Logger.getLogger(Config.class);

    /** Name of file that holds the config data*/
    private static final String CONF_FILE = "zab.config";

    /**The duration of tick in milliseconds */
    private static final String TICK_TIME_KEY = "tickTime";
    /**Number of tick that should take a leader to have quorum*/
    private static final String INIT_LIMIT_KEY = "initLimit";
    /**Maximum  number of tick a follower may be behind to be considered synced*/
    private static final String SYNC_LIMIT_KEY = "syncLimit";
    /**The addresses for quorum servers in the format &lt;host&gt;:&lt;protocol port&gt;:&lt;election port&gt;*/
    private static final String SERVER_KEY_PREFIX = "server.";
    /** Whether disable Nagle's Algorithm for TCP, improving latency for small packets */
    private static final String TCP_NODELAY_KEY = "tcpDelay";
    /** How many times follower must try to connect to leader before giving up */
    private static final String FOLLOWER_MAX_ATTEMPS_CONNECT_KEY = "followerMaxConnAttempts";
    /** Initial time for the exponential backoff of follower connecting to leader */
    private static final String FOLLOWER_INITIAL_BACKOFF_KEY = "followerInitialBackoff";

    private int tickTime = 200;
    private int syncLimit = 2;
    private int initLimit = 5;
    private boolean tcpDelay = true;
    private int flMaxConnAttempts = 5;
    private int flInitialBackoff = 500;
    private Map<Long,QuorumServerSettings> quorumServers = new HashMap <Long,QuorumServerSettings>();

    private static volatile Config instance = null;

    private Config(){
        load();
    }

    private void load() {
        Properties properties = new Properties();
        URL configFileUrl = ClassLoader.getSystemResource(CONF_FILE);
        if (configFileUrl != null) {
            FileInputStream configFileStream = null;
            try {
                configFileStream = new FileInputStream(configFileUrl.getFile());
                properties.load(configFileStream);
                parse(properties);
            } catch (FileNotFoundException e) {
                LOG.error("The config file was not found", e);
            } catch (IOException e) {
                LOG.error("Some problem when reading the config file", e);
            } finally {
                if (configFileStream != null) {
                    try {
                        configFileStream.close();
                    } catch (IOException e) {
                        LOG.warn("Some problem when closing config file", e);
                    }
                }
            }
        }
    }

    private void parse(Properties properties){

        for (Entry<Object, Object> entry : properties.entrySet()){
            String key = entry.getKey().toString().trim();
            String value = entry.getValue().toString().trim();

            if(key.equals(TICK_TIME_KEY)){
                tickTime = Integer.parseInt(value);
            }else if(key.equals(INIT_LIMIT_KEY)){
                initLimit =  Integer.parseInt(value);
            }else if(key.equals(SYNC_LIMIT_KEY)){
                syncLimit =  Integer.parseInt(value);
            }else if(key.equals(TCP_NODELAY_KEY)){
                tcpDelay = Boolean.parseBoolean(value);
            }else if(key.endsWith(FOLLOWER_MAX_ATTEMPS_CONNECT_KEY)){
                flMaxConnAttempts = Integer.parseInt(value);
            }else if(key.equals(FOLLOWER_INITIAL_BACKOFF_KEY)){
                flInitialBackoff = Integer.parseInt(value);
            }else if(key.startsWith(SERVER_KEY_PREFIX)){
                long serverId = Long.parseLong(key.substring(key.indexOf('.')+1));
                String parts[] = value.split(":");
                if (parts.length != 3){
                    LOG.error("Config for server "+ serverId + " is wrong: " + value);
                    continue;
                }
                InetSocketAddress serverAddress = new InetSocketAddress(parts[0], Integer.parseInt(parts[1]));
                InetSocketAddress electionAddress = new InetSocketAddress(parts[0], Integer.parseInt(parts[2]));

                QuorumServerSettings serverSettings = new QuorumServerSettings(serverId,serverAddress,electionAddress);
                quorumServers.put(serverId, serverSettings);
            }
        }

    }


    public static Config getInstance(){
        if(instance == null){
            synchronized(Config.class){
                //Double check  thread-safe singleton patter
                if(instance == null){
                    instance = new Config();
                }
            }
        }
        return instance;
    }

    public int getTickTime() {return tickTime;}
    public int getSyncLimit() {return syncLimit;}
    public int getInitLimit() {return initLimit;}
    public boolean getTcpDelay() {return tcpDelay;}
    public int getFollowerMaxConnAttempts() {return flMaxConnAttempts;}
    public int getFollowerInitialBackoff() {return flInitialBackoff;}
    public Map<Long,QuorumServerSettings> getQuorumServers() {return quorumServers;}

}

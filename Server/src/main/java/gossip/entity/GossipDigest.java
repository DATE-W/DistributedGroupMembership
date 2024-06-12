package gossip.entity;

import lombok.Data;

import java.io.Serializable;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;

@Data
public class GossipDigest implements Serializable, Comparable<GossipDigest> {
    private InetSocketAddress endpoint;
    private long heartbeatTime;
    private long version;
    private String id;

    public GossipDigest(){}

    public GossipDigest(GossipMember endpoint, long heartbeatTime, long version) throws UnknownHostException {
        this.endpoint = new InetSocketAddress(InetAddress.getByName(endpoint.getIpAddress()), endpoint.getPort());
        this.heartbeatTime = heartbeatTime;
        this.version = version;
        this.id = endpoint.getId();
    }

    /**
     * 比较新旧
     *
     * @param digest 所要比较信息
     * @return int 比较结果 - 大于零则大于，小于零则小于，等于零则相等
     */
    @Override
    public int compareTo(GossipDigest digest) {
        if (heartbeatTime != digest.heartbeatTime) {
            return (int) (heartbeatTime - digest.heartbeatTime);
        }
        return (int) (version - digest.version);
    }

    @Override
    public String toString() {
        return "GossipDigest{" +
                "endpoint=" + endpoint.toString() +
                ", heartbeatTime=" + heartbeatTime +
                ", version=" + version +
                '}';
    }
}

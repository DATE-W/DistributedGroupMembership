package gossip.entity;

import lombok.Data;

import java.io.Serializable;
import java.util.Optional;

@Data
public class SeedMember implements Serializable {
    private String cluster;
    private String ipAddress;
    private Integer port;
    private String id;

    public SeedMember(String cluster, String ipAddress, Integer port, String id) {
        this.cluster = cluster;
        this.ipAddress = ipAddress;
        this.port = port;
        this.id = id;
    }

    public SeedMember(){}

    /**
     * 特征值
     * cluster+ip+port
     *
     * @return {@code String}
     */
    public String eigenvalue() {
        return getCluster().concat(":").concat(getIpAddress()).concat(":").concat(getPort().toString());
    }

    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        SeedMember member = (SeedMember) o;

        if (!cluster.equals(member.cluster)) {
            return false;
        }
        if (!ipAddress.equals(member.ipAddress)) {
            return false;
        }
        return port.equals(member.port);
    }

    @Override
    public int hashCode() {
        int result = cluster.hashCode();
        result = 31 * result + ipAddress.hashCode();
        result = 31 * result + port.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "SeedMember{" +
                "cluster='" + cluster + '\'' +
                ", ipAddress='" + ipAddress + '\'' +
                ", port=" + port +
                ", id='" + id + '\'' +
                '}';
    }
}

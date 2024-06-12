package gossip.entity;

import gossip.manager.VersionManager;
import lombok.Data;

@Data
public class HeartbeatState {
    private long heartbeatTime;
    private long version;

    public HeartbeatState() {
        this.heartbeatTime = System.currentTimeMillis();
    }

    public long updateVersion() {
        setHeartbeatTime(System.currentTimeMillis());
        this.version = VersionManager.getInstance().nextVersion();
        return version;
    }

    @Override
    public String toString() {
        return "HeartbeatState{" +
                "heartbeatTime=" + heartbeatTime +
                ", version=" + version +
                '}';
    }
}

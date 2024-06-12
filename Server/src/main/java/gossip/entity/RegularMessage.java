package gossip.entity;

import lombok.Data;

import java.io.Serializable;

@Data
public class RegularMessage implements Serializable {
    private static final long DEFAULT_TTL = 300000;
    private String id;
    private long ttl;
    private long createTime;
    private Object payload;
    private int forwardCount;

    private GossipMember creator;

    public RegularMessage() {}

    public RegularMessage(GossipMember creator, Object payload) {
        this(creator, payload, DEFAULT_TTL);
    }

    public RegularMessage(GossipMember creator, Object payload, Long ttl) {
        long now = System.currentTimeMillis();
        this.ttl = ttl == null ? DEFAULT_TTL : ttl;
        this.creator = creator;
        this.payload = payload;
        this.id = "REG_MSG_" + now;
        this.createTime = now;
        this.forwardCount = 0;
    }

    @Override
    public String toString() {
        return "RegularMessage{" +
                "id='" + id + '\'' +
                ", ttl=" + ttl +
                ", createTime=" + createTime +
                ", payload=" + payload +
                ", forwardCount=" + forwardCount +
                ", creator=" + creator +
                '}';
    }
}

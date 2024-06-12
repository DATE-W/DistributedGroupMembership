package gossip.entity;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import gossip.serializer.CustomDeserializer;
import gossip.serializer.CustomSerializer;
import lombok.Data;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

@Data
public class AckMessage implements Serializable {
    private List<GossipDigest> olders;

    @JsonSerialize(keyUsing = CustomSerializer.class)
    @JsonDeserialize(keyUsing = CustomDeserializer.class)
    private Map<GossipMember, HeartbeatState> newers;

    public AckMessage(){}

    public AckMessage(List<GossipDigest> olders, Map<GossipMember, HeartbeatState> newers) {
        this.olders = olders;
        this.newers = newers;
    }

    @Override
    public String toString() {
        return "AckMessage{" +
                "olders=" + olders +
                ", newers=" + newers +
                '}';
    }
}

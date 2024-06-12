package gossip.handler;

import gossip.entity.*;
import gossip.manager.GossipManager;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonObject;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class AckMessageHandler implements MessageHandler {
    @Override
    public void handle(String cluster, String data, String from) {
        JsonObject dj = new JsonObject(data);
        AckMessage ackMessage = dj.mapTo(AckMessage.class);

        List<GossipDigest> olders = ackMessage.getOlders();
        Map<GossipMember, HeartbeatState> newers = ackMessage.getNewers();

        //更新本地状态信息
        if (newers.size() > 0) {
            GossipManager.getInstance().apply2LocalState(newers);
        }

        Map<GossipMember, HeartbeatState> deltaEndpoints = new HashMap<>();
        if (olders != null) {
            for (GossipDigest d : olders) {
                GossipMember member = GossipManager.getInstance().createByDigest(d);
                HeartbeatState hb = GossipManager.getInstance().getEndpointMembers().get(member);
                if (hb != null) {
                    deltaEndpoints.put(member, hb);
                }
            }
        }

        if (!deltaEndpoints.isEmpty()) {
            Ack2Message ack2Message = new Ack2Message(deltaEndpoints);
            Buffer ack2Buffer = GossipManager.getInstance().encodeAck2Message(ack2Message);
            if (from != null) {
                String[] host = from.split(":");
                GossipManager.getInstance().getSettings().getMsgService().sendMsg(host[0], Integer.valueOf(host[1]), ack2Buffer);
            }
        }
    }
}

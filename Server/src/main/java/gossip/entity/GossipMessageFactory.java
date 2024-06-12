package gossip.entity;

import gossip.constants.MessageType;
import io.vertx.core.json.JsonObject;

public class GossipMessageFactory {
    // json属性常量
    public static final String KEY_MSG_TYPE = "msgtype";

    public static final String KEY_DATA = "data";
    public static final String KEY_CLUSTER = "cluster";
    public static final String KEY_FROM = "from";
    private static final GossipMessageFactory ourInstance = new GossipMessageFactory();

    private GossipMessageFactory(){}

    public static GossipMessageFactory getInstance() {
        return ourInstance;
    }

    /**
     * 生成消息对象
     *
     * @param type    消息类型
     * @param data    数据
     * @param cluster 集群
     * @param from    消息源结点
     * @return {@code JsonObject}
     */
    public JsonObject makeMessage(MessageType type, String data, String cluster, String from) {
        JsonObject bj = new JsonObject();
        bj.put(KEY_MSG_TYPE, type);
        bj.put(KEY_CLUSTER, cluster);
        bj.put(KEY_DATA, data);
        bj.put(KEY_FROM, from);
        return bj;
    }
}

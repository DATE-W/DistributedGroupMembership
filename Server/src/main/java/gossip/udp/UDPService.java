package gossip.udp;

import gossip.constants.MessageType;
import gossip.entity.GossipMessageFactory;
import gossip.handler.MessageHandler;
import io.netty.util.internal.StringUtil;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.datagram.DatagramSocket;
import io.vertx.core.datagram.DatagramSocketOptions;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;


public class UDPService {
    Logger LOGGER = LoggerFactory.getLogger(UDPService.class);
    DatagramSocket socket;

    /**
     * 开启监听
     *
     * @param ipAddress ip地址
     * @param port      端口
     */
    public void startListen(String ipAddress, int port) {
        DatagramSocketOptions options = new DatagramSocketOptions();
        options.setReceiveBufferSize(65535);
        socket = Vertx.vertx().createDatagramSocket(options);
        socket.listen(port, ipAddress, asyncResult -> {
            if (asyncResult.succeeded()) {
                socket.handler(packet -> handleMsg(packet.data()));
            } else {
                LOGGER.error("监听失败 " + asyncResult.cause());
            }
        });
    }

    public void handleMsg(Buffer data) {
        // 解析消息
        JsonObject j = data.toJsonObject();
        String msgType = j.getString(GossipMessageFactory.KEY_MSG_TYPE);
        String _data = j.getString(GossipMessageFactory.KEY_DATA);
        String cluster = j.getString(GossipMessageFactory.KEY_CLUSTER);
        String from = j.getString(GossipMessageFactory.KEY_FROM);
        if (StringUtil.isNullOrEmpty(cluster) || !GossipManager.getInstance().getCluster().equals(cluster)) {
            LOGGER.error("此信息不满足格式要求：" + data);
            return;
        }
        // 判断消息类型
        MessageHandler handler = null;
        MessageType type = MessageType.valueOf(msgType);
        if (type == MessageType.SYNC_MESSAGE) {
            handler = new SyncMessageHandler();
        } else if (type == MessageType.ACK_MESSAGE) {
            handler = new AckMessageHandler();
        } else if (type == MessageType.ACK2_MESSAGE) {
            handler = new Ack2MessageHandler();
        } else if (type == MessageType.SHUTDOWN) {
            handler = new ShutdownMessageHandler();
        } else if (type == MessageType.REG_MESSAGE) {
            handler = new RegularMessageHandler();
        } else {
            LOGGER.error("信息类型错误");
        }
        if (handler != null) {
            handler.handle(cluster, _data, from);
        }
    }

    public void sendMsg(String targetIp, Integer targetPort, Buffer data) {
        if (targetIp != null && targetPort != null && data != null) {
            socket.send(data, targetPort, targetIp, asyncResult -> {
            });
        }
    }

    public void unListen() {
        if (socket != null) {
            socket.close(asyncResult -> {
                if (asyncResult.succeeded()) {
                    LOGGER.info("Socket 已关闭");
                } else {
                    LOGGER.error("Socket关闭出错： " + asyncResult.cause().getMessage());
                }
            });
        }
    }
}

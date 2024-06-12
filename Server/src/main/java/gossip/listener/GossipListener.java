package gossip.listener;


import gossip.constants.GossipState;
import gossip.entity.GossipMember;
import gossip.manager.GossipManager;

/**
 * 定义Gossip事件监听器
 * 用于监听Gossip事件
 * 事件包括：节点加入、节点离开、节点发布信息
 *
 */
public interface GossipListener {
    void gossipEvent(GossipMember member, GossipState state, Object payload, GossipManager selfManager);
}

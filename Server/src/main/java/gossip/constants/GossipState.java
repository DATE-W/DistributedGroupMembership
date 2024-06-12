package gossip.constants;

/**
 * 结点状态
 *
 * @author Pikachudy
 * @date 2022/12/08
 */
public enum GossipState {
    MEET("meet"),UP("up"), DOWN("down"), JOIN("join"), RCV("receive");

    private final String state;

    GossipState(String state) {
        this.state = state;
    }

}

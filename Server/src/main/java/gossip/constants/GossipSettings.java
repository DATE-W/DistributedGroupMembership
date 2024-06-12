package gossip.constants;

import gossip.entity.SeedMember;
import gossip.manager.GossipManager;
import gossip.manager.MessageManager;
import gossip.udp.UDPService;
import lombok.Data;

import java.util.ArrayList;
import java.util.List;

@Data
public class GossipSettings {
    // 随机ping的时间间隔
    private int gossipInterval = 1000;

    // 网络超时时间
    private int networkDelay = 0;

    private UDPService msgService = new UDPService();

    // 删除的阈值
    private int deleteThreshold = 3;

    private List<SeedMember> seedMembers;

    private MessageManager messageManager = new MessageManager();

    private String LOG_PATH = "Server/src/main/java/log/{0}_{1}.log";

    public void setSeedMembers(List<SeedMember> seedMembers) {
        List<SeedMember> _seedMembers = new ArrayList<>();
        if (seedMembers != null && !seedMembers.isEmpty()) {
            for (SeedMember seedMember : seedMembers) {
                if (!seedMember.eigenvalue().equalsIgnoreCase(GossipManager.getInstance().getSelf().eigenvalue())) {
                    if (!_seedMembers.contains(seedMember)) {
                        _seedMembers.add(seedMember);
                    }
                }
            }
        }
        this.seedMembers = seedMembers;
    }
}

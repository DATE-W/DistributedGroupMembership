package gossip.manager;

import java.util.concurrent.atomic.AtomicLong;

/**
 * 版本管理
 *
 * @author Pikachudy
 * @date 2022/12/09
 */
public class VersionManager {
    private static final AtomicLong v = new AtomicLong(0);
    private static final VersionManager instance = new VersionManager();

    private VersionManager() {
    }

    public static VersionManager getInstance() {
        return instance;
    }

    public long nextVersion() {
        return v.incrementAndGet();
    }
}

package gossip.manager;

import gossip.constants.GossipSettings;
import gossip.constants.GossipState;
import gossip.constants.MessageType;
import gossip.entity.*;
import gossip.listener.GossipListener;
import gossip.serializer.Serializer;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import lombok.Data;

import java.io.IOException;
import java.net.UnknownHostException;
import java.text.MessageFormat;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.logging.FileHandler;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

@Data
public class GossipManager {
    // 日志记录器
    private static final Logger LOGGER = Logger.getLogger(GossipManager.class.getName());
    // 单例模式实例
    private static final GossipManager instance = new GossipManager();
    // 读写锁
    private final ReentrantReadWriteLock reentrantReadWriteLock = new ReentrantReadWriteLock();
    // 定时任务线程池
    private final ScheduledExecutorService doGossipExecutor = Executors.newScheduledThreadPool(1);
    // 存储所有成员的状态信息
    private final Map<GossipMember, HeartbeatState> endpointMembers = new ConcurrentHashMap<>();
    private final List<GossipMember> liveMembers = new ArrayList<>();
    private final List<GossipMember> deadMembers = new ArrayList<>();
    private final Map<GossipMember, CandidateMemberState> candidateMembers = new ConcurrentHashMap<>();
    // 随机数生成器
    private final Random random = new Random();
    // 标识服务是否在工作
    private boolean isWorking = false;
    // 标识是否是种子节点
    private Boolean isSeedNode = null;
    // 配置信息
    private GossipSettings settings;
    // 本地节点信息
    private GossipMember localGossipMember;
    // 集群名称
    private String cluster;
    // 事件监听器
    private GossipListener listener;

    // 私有构造函数，单例模式
    private GossipManager() {}

    // 获取单例实例
    public static GossipManager getInstance() {
        return instance;
    }

    /**
     * 初始化Gossip管理器
     */
    public void init(String cluster, String ipAddress, Integer port, String id, List<SeedMember> seedMembers, GossipSettings settings, GossipListener listener) throws IOException {
        this.cluster = cluster;
        this.localGossipMember = new GossipMember();
        this.localGossipMember.setCluster(cluster);
        this.localGossipMember.setIpAddress(ipAddress);
        this.localGossipMember.setPort(port);
        this.localGossipMember.setId(id);
        this.localGossipMember.setState(GossipState.JOIN);
        this.endpointMembers.put(localGossipMember, new HeartbeatState());
        this.listener = listener;
        this.settings = settings;
        this.settings.setSeedMembers(seedMembers);
        this.LOGGER.setUseParentHandlers(false);
        try {
            String filePath = MessageFormat.format(getSettings().getLOG_PATH(), cluster, this.localGossipMember.getId().replace(":", "_"));
            // 判断文件是否存在, 若不存在则新建
            if(!new java.io.File(filePath).exists()){
                new java.io.File(filePath).createNewFile();
            }
            FileHandler fileHandler = new FileHandler(filePath);
            fileHandler.setFormatter(new SimpleFormatter());
            this.LOGGER.addHandler(fileHandler);
        } catch (SecurityException e) {
            throw new RuntimeException(e);
        }
        // 触发Gossip事件，节点加入
        fireGossipEvent(localGossipMember, GossipState.JOIN);
    }

    /**
     * 开始Gossip服务
     */
    protected void start() {
        LOGGER.info(String.format("正在开启服务…… cluster[%s] ip[%s] port[%d] id[%s]", localGossipMember.getCluster(), localGossipMember.getIpAddress(), localGossipMember.getPort(), localGossipMember.getId()));
        isWorking = true;
        // 开始监听消息服务
        settings.getMsgService().startListen(getSelf().getIpAddress(), getSelf().getPort());
        // 定时任务执行Gossip任务
        doGossipExecutor.scheduleAtFixedRate(new GossipTask(), settings.getGossipInterval(), settings.getGossipInterval(), TimeUnit.MILLISECONDS);
    }

    public GossipMember getSelf() {
        return localGossipMember;
    }

    public String getID() {
        return getSelf().getId();
    }

    public Map<GossipMember, HeartbeatState> getEndpointMembers() {
        return endpointMembers;
    }

    public String getCluster() {
        return cluster;
    }

    /**
     * 随机生成Gossip摘要
     */
    private void randomGossipDigest(List<GossipDigest> digests) throws UnknownHostException {
        List<GossipMember> endpoints = new ArrayList<>(endpointMembers.keySet());
        Collections.shuffle(endpoints, random);
        for (GossipMember ep : endpoints) {
            HeartbeatState hb = endpointMembers.get(ep);
            long hbTime = 0;
            long hbVersion = 0;
            if (hb != null) {
                hbTime = hb.getHeartbeatTime();
                hbVersion = hb.getVersion();
            }
            digests.add(new GossipDigest(ep, hbTime, hbVersion));
        }
    }

    /**
     * 编码同步消息
     */
    private Buffer encodeSyncMessage(List<GossipDigest> digests) {
        Buffer buffer = Buffer.buffer();
        JsonArray array = new JsonArray();
        for (GossipDigest e : digests) {
            array.add(Serializer.getInstance().encode(e).toString());
        }
        buffer.appendString(GossipMessageFactory.getInstance().makeMessage(MessageType.SYNC_MESSAGE, array.encode(), getCluster(), getSelf().ipSplicePort()).encode());
        return buffer;
    }

    /**
     * 编码ACK消息
     */
    public Buffer encodeAckMessage(AckMessage ackMessage) {
        Buffer buffer = Buffer.buffer();
        JsonObject ackJson = JsonObject.mapFrom(ackMessage);
        buffer.appendString(GossipMessageFactory.getInstance().makeMessage(MessageType.ACK_MESSAGE, ackJson.encode(), getCluster(), getSelf().ipSplicePort()).encode());
        return buffer;
    }

    /**
     * 编码ACK2消息
     */
    public Buffer encodeAck2Message(Ack2Message ack2Message) {
        Buffer buffer = Buffer.buffer();
        JsonObject ack2Json = JsonObject.mapFrom(ack2Message);
        buffer.appendString(GossipMessageFactory.getInstance().makeMessage(MessageType.ACK2_MESSAGE, ack2Json.encode(), getCluster(), getSelf().ipSplicePort()).encode());
        return buffer;
    }

    /**
     * 编码关闭消息
     */
    private Buffer encodeShutdownMessage() {
        Buffer buffer = Buffer.buffer();
        JsonObject self = JsonObject.mapFrom(getSelf());
        buffer.appendString(GossipMessageFactory.getInstance().makeMessage(MessageType.SHUTDOWN, self.encode(), getCluster(), getSelf().ipSplicePort()).encode());
        return buffer;
    }

    /**
     * 编码常规消息
     */
    private Buffer encodeRegularMessage(RegularMessage regularMessage) {
        Buffer buffer = Buffer.buffer();
        JsonObject msg = JsonObject.mapFrom(regularMessage);
        buffer.appendString(GossipMessageFactory.getInstance().makeMessage(MessageType.REG_MESSAGE, msg.encode(), getCluster(), getSelf().ipSplicePort()).encode());
        return buffer;
    }

    /**
     * 将远程状态应用到本地状态
     */
    public void apply2LocalState(Map<GossipMember, HeartbeatState> endpointMembers) {
        Set<GossipMember> keys = endpointMembers.keySet();
        for (GossipMember m : keys) {
            if (getSelf().equals(m)) {
                continue;
            }

            try {
                HeartbeatState localState = getEndpointMembers().get(m);
                HeartbeatState remoteState = endpointMembers.get(m);

                if (localState != null) {
                    long localHeartbeatTime = localState.getHeartbeatTime();
                    long remoteHeartbeatTime = remoteState.getHeartbeatTime();
                    if (remoteHeartbeatTime > localHeartbeatTime) {
                        remoteStateReplaceLocalState(m, remoteState);
                    } else if (remoteHeartbeatTime == localHeartbeatTime) {
                        long localVersion = localState.getVersion();
                        long remoteVersion = remoteState.getVersion();
                        if (remoteVersion > localVersion) {
                            remoteStateReplaceLocalState(m, remoteState);
                        }
                    }
                } else {
                    remoteStateReplaceLocalState(m, remoteState);
                }
            } catch (Exception e) {
                LOGGER.severe(e.getMessage());
            }
        }
    }

    /**
     * 替换本地状态为远程状态
     */
    private void remoteStateReplaceLocalState(GossipMember member, HeartbeatState remoteState) {
        if (member.getState() == GossipState.UP) {
            up(member);
        }
        if (member.getState() == GossipState.DOWN) {
            down(member);
        }
        if (endpointMembers.containsKey(member)) {
            endpointMembers.remove(member);
        }
        endpointMembers.put(member, remoteState);
    }

    /**
     * 通过Gossip摘要创建成员
     */
    public GossipMember createByDigest(GossipDigest digest) {
        GossipMember member = new GossipMember();
        member.setPort(digest.getEndpoint().getPort());
        member.setIpAddress(digest.getEndpoint().getAddress().getHostAddress());
        member.setCluster(cluster);

        Set<GossipMember> keys = getEndpointMembers().keySet();
        for (GossipMember m : keys) {
            if (m.equals(member)) {
                member.setId(m.getId());
                member.setState(m.getState());
                break;
            }
        }

        return member;
    }

    /**
     * 发送sync信息至活结点
     */
    private boolean gossip2LiveMember(Buffer buffer) {
        int liveSize = liveMembers.size();
        if (liveSize <= 0) {
            return false;
        }
        boolean b = false;
        int c = Math.min(liveSize, convergenceCount());
        for (int i = 0; i < c; i++) {
            int index = random.nextInt(liveSize);
            b = b || sendGossip(buffer, liveMembers, index);
        }
        return b;
    }

    /**
     * 发送sync信息至死结点
     */
    private void gossip2UndiscoverableMember(Buffer buffer) {
        int deadSize = deadMembers.size();
        if (deadSize <= 0) {
            return;
        }
        int index = (deadSize == 1) ? 0 : random.nextInt(deadSize);
        sendGossip(buffer, deadMembers, index);
    }

    /**
     * 发送sync信息至种子节点
     */
    private void gossip2Seed(Buffer buffer) {
        int size = settings.getSeedMembers().size();
        if (size > 0) {
            if (size == 1 && isSeedNode()) {
                return;
            }
            int index = (size == 1) ? 0 : random.nextInt(size);
            if (liveMembers.size() == 1) {
                sendGossip2Seed(buffer, settings.getSeedMembers(), index);
            } else {
                double prob = size / (double) liveMembers.size();
                if (random.nextDouble() < prob) {
                    sendGossip2Seed(buffer, settings.getSeedMembers(), index);
                }
            }
        }
    }

    /**
     * 发送Gossip信息
     */
    private boolean sendGossip(Buffer buffer, List<GossipMember> members, int index) {
        if (buffer != null && index >= 0) {
            try {
                GossipMember target = members.get(index);
                if (target.equals(getSelf())) {
                    int m_size = members.size();
                    if (m_size == 1) {
                        return false;
                    } else {
                        target = members.get((index + 1) % m_size);
                    }
                }
                settings.getMsgService().sendMsg(target.getIpAddress(), target.getPort(), buffer);
                return settings.getSeedMembers().contains(gossipMember2SeedMember(target));
            } catch (Exception e) {
                LOGGER.severe(e.getMessage());
            }
        }
        return false;
    }

    /**
     * 发送Gossip信息到种子节点
     */
    private boolean sendGossip2Seed(Buffer buffer, List<SeedMember> members, int index) {
        if (buffer != null && index >= 0) {
            try {
                SeedMember target = members.get(index);
                int m_size = members.size();
                if (target.equals(gossipMember2SeedMember(getSelf()))) {
                    if (m_size <= 1) {
                        return false;
                    } else {
                        target = members.get((index + 1) % m_size);
                    }
                }
                settings.getMsgService().sendMsg(target.getIpAddress(), target.getPort(), buffer);
                return true;
            } catch (Exception e) {
                LOGGER.severe(e.getMessage());
            }
        }
        return false;
    }

    /**
     * 将GossipMember转换为SeedMember
     */
    private SeedMember gossipMember2SeedMember(GossipMember member) {
        return new SeedMember(member.getCluster(), member.getIpAddress(), member.getPort(), member.getId());
    }

    /**
     * 检查各个节点的心跳时间，如果超过阈值则认为该节点已经 down
     */
    private void checkStatus() {
        try {
            GossipMember local = getSelf();
            Map<GossipMember, HeartbeatState> endpoints = getEndpointMembers();
            Set<GossipMember> epKeys = endpoints.keySet();
            for (GossipMember k : epKeys) {
                if (!k.equals(local)) {
                    HeartbeatState state = endpoints.get(k);
                    long now = System.currentTimeMillis();
                    long duration = now - state.getHeartbeatTime();
                    long convictedTime = convictedTime();
                    LOGGER.info("检测 : " + k + " 状态 : " + state + " 延时 : " + duration + " 用时 : " + convictedTime);
                    if (duration > convictedTime && (isAlive(k) || getLiveMembers().contains(k))) {
                        downing(k, state);
                    }
                    if (duration <= convictedTime && (isDiscoverable(k) || getDeadMembers().contains(k))) {
                        up(k);
                    }
                }
            }
            checkCandidate();
        } catch (Exception e) {
            LOGGER.severe(e.getMessage());
        }
    }

    /**
     * 计算收敛数
     */
    private int convergenceCount() {
        int size = getEndpointMembers().size();
        return (int) Math.floor(Math.log10(size) + Math.log(size) + 1);
    }

    /**
     * 计算判定时间
     */
    private long convictedTime() {
        long executeGossipTime = 500;
        return ((convergenceCount() * (settings.getNetworkDelay() * 3L + executeGossipTime)) << 1) + settings.getGossipInterval();
    }

    /**
     * 判断节点是否可发现
     */
    private boolean isDiscoverable(GossipMember member) {
        return member.getState() == GossipState.JOIN || member.getState() == GossipState.DOWN;
    }

    /**
     * 判断节点是否存活
     */
    private boolean isAlive(GossipMember member) {
        return member.getState() == GossipState.UP;
    }

    /**
     * 判断是否为种子节点
     */
    public boolean isSeedNode() {
        if (isSeedNode == null) {
            isSeedNode = settings.getSeedMembers().contains(gossipMember2SeedMember(getSelf()));
        }
        return isSeedNode;
    }

    /**
     * 触发Gossip事件
     */
    private void fireGossipEvent(GossipMember member, GossipState state) {
        fireGossipEvent(member, state, null);
    }

    /**
     * 发送事件
     */
    public void fireGossipEvent(GossipMember member, GossipState state, Object payload) {
        if (getListener() != null) {
            if (state == GossipState.RCV) {
                new Thread(() -> getListener().gossipEvent(member, state, payload, this)).start();
            } else {
                getListener().gossipEvent(member, state, payload, this);
            }
        }
    }

    /**
     * 节点下线
     */
    public void down(GossipMember member) {
        LOGGER.info("节点断开！");
        try {
            reentrantReadWriteLock.writeLock().lock();
            member.setState(GossipState.DOWN);
            liveMembers.remove(member);
            if (!deadMembers.contains(member)) {
                deadMembers.add(member);
            }
            fireGossipEvent(member, GossipState.DOWN);
        } catch (Exception e) {
            LOGGER.severe(e.getMessage());
        } finally {
            reentrantReadWriteLock.writeLock().unlock();
        }
    }

    /**
     * 节点上线
     */
    private void up(GossipMember member) {
        try {
            boolean isMeet = false;
            reentrantReadWriteLock.writeLock().lock();
            member.setState(GossipState.UP);
            if (!liveMembers.contains(member)) {
                liveMembers.add(member);
                isMeet = true;
            }
            if (candidateMembers.containsKey(member)) {
                candidateMembers.remove(member);
            }
            if (deadMembers.contains(member)) {
                // 重新上线
                deadMembers.remove(member);
                LOGGER.info("节点上线！！");
                if (!member.equals(getSelf())) {
                    fireGossipEvent(member, GossipState.UP);
                }
            }
            else {
                if(isMeet) {
                    // 首次加入
                    LOGGER.info("节点加入！！");
                    if (!member.equals(getSelf())) {
                        fireGossipEvent(member, GossipState.MEET);
                    }
                }
            }

        } catch (Exception e) {
            LOGGER.severe(e.getMessage());
        } finally {
            reentrantReadWriteLock.writeLock().unlock();
        }

    }

    /**
     * 节点正在下线
     */
    private void downing(GossipMember member, HeartbeatState state) {
        LOGGER.info("正在下线……");
        try {
            if (candidateMembers.containsKey(member)) {
                CandidateMemberState cState = candidateMembers.get(member);
                if (state.getHeartbeatTime() == cState.getHeartbeatTime()) {
                    cState.updateCount();
                } else if (state.getHeartbeatTime() > cState.getHeartbeatTime()) {
                    candidateMembers.remove(member);
                }
            } else {
                candidateMembers.put(member, new CandidateMemberState(state.getHeartbeatTime()));
            }
        } catch (Exception e) {
            LOGGER.severe(e.getMessage());
        }
    }

    /**
     * 检查候选成员
     */
    private void checkCandidate() {
        Set<GossipMember> keys = candidateMembers.keySet();
        for (GossipMember m : keys) {
            if (candidateMembers.get(m).getDowningCount().get() >= convergenceCount()) {
                down(m);
                candidateMembers.remove(m);
            }
        }
    }

    /**
     * 关闭Gossip服务
     */
    protected void shutdown() {
        getSettings().getMsgService().unListen();
        doGossipExecutor.shutdown();
        try {
            Thread.sleep(getSettings().getGossipInterval());
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        Buffer buffer = encodeShutdownMessage();
        for (int i = 0; i < getLiveMembers().size(); i++) {
            sendGossip(buffer, getLiveMembers(), i);
        }
        isWorking = false;
    }

    /**
     * 发布消息
     */
    public void publish(Object payload) {
        RegularMessage msg = new RegularMessage(getSelf(), payload, convictedTime());
        settings.getMessageManager().add(msg);
    }

    /**
     * 显示摘要信息
     */
    public void showDigests() {
        System.out.println("------------------------------------------------------------------------------");
        System.out.println("当前节点 :\n\t" + getSelf());
        System.out.println("活跃节点 :" );
        for(GossipMember member : getLiveMembers()) {
            System.out.println("\t"+member);
        }
        System.out.println("死亡节点 :");
        for(GossipMember member : getDeadMembers()) {
            System.out.println("\t"+member);
        }
        System.out.println("------------------------------------------------------------------------------\n");

    }

    /**
     * Gossip任务
     */
    class GossipTask implements Runnable {

        @Override
        public void run() {
            // 更新本地版本信息
            long version = endpointMembers.get(getSelf()).updateVersion();
            if (isDiscoverable(getSelf())) {
                up(getSelf());
            }
            LOGGER.info("同步数据");
            LOGGER.info(String.format("更新后的 heartbeat version为 %d", version));

            List<GossipDigest> digests = new ArrayList<>();
            try {
                randomGossipDigest(digests);
                if (digests.size() > 0) {
                    Buffer syncMessageBuffer = encodeSyncMessage(digests);
                    sendBuf(syncMessageBuffer);
                }
                checkStatus();
                LOGGER.info("live member : " + getLiveMembers());
                LOGGER.info("dead member : " + getDeadMembers());
                LOGGER.info("endpoint : " + getEndpointMembers());
                if(version == 2){
                    showDigests(); // 初始化打印信息
                }
                new Thread(() -> {
                    MessageManager mm = settings.getMessageManager();
                    if (!mm.isEmpty()) {
                        for (String id : mm.list()) {
                            RegularMessage msg = mm.acquire(id);
                            int c = msg.getForwardCount();
                            int maxTry = convergenceCount();
                            if (c < maxTry) {
                                sendBuf(encodeRegularMessage(msg));
                                msg.setForwardCount(c + 1);
                            }
                            if ((System.currentTimeMillis() - msg.getCreateTime()) >= msg.getTtl()) {
                                mm.remove(id);
                            }
                        }
                    }
                }).start();
            } catch (UnknownHostException e) {
                LOGGER.severe(e.getMessage());
            }

        }

        /**
         * 发送消息缓冲区
         */
        private void sendBuf(Buffer buf) {
            // 随机 live 结点
            boolean b = gossip2LiveMember(buf);

            // 从 dead 结点中取一个
            gossip2UndiscoverableMember(buf);

            // 如果没有发送到任何live节点或者live节点数量少于种子节点数量，发送到种子节点
            if (!b || liveMembers.size() <= settings.getSeedMembers().size()) {
                gossip2Seed(buf);
            }
        }
    }
}

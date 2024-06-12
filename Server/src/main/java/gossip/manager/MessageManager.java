package gossip.manager;

import gossip.entity.RegularMessage;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class MessageManager {
    private static final ConcurrentHashMap<String, RegularMessage> RegMessages = new ConcurrentHashMap<>();

    public void add(RegularMessage msg) {
        RegMessages.putIfAbsent(msg.getId(), msg);
    }

    public RegularMessage acquire(String id) {
        return RegMessages.get(id);
    }

    public RegularMessage remove(String id) {
        return RegMessages.remove(id);
    }

    public boolean contains(String id) {
        return RegMessages.containsKey(id);
    }

    public boolean isEmpty() {
        return RegMessages.isEmpty();
    }

    public Set<String> list() {
        return RegMessages.keySet();
    }
}

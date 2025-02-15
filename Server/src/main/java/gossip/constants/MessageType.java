package gossip.constants;

public enum MessageType {
    SYNC_MESSAGE("sync_message"), ACK_MESSAGE("ack_message"), ACK2_MESSAGE("ack2_message"), SHUTDOWN("shutdown"), REG_MESSAGE("reg_message");

    private final String type;

    MessageType(String type) {
        this.type = type;
    }
}
package gossip.handler;

import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;

public interface MessageHandler {
    Logger LOGGER = LoggerFactory.getLogger(MessageHandler.class);

    void handle(String cluster, String data, String from);
}
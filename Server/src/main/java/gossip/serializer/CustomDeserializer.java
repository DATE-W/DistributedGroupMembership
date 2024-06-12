package gossip.serializer;

import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.KeyDeserializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import gossip.entity.GossipMember;

import java.io.IOException;

/**
 * 自定义反序列化
 *
 * @author Pikachudy
 * @date 2022/12/10
 */
public class CustomDeserializer extends KeyDeserializer {
    ObjectMapper mapper = new ObjectMapper();

    public CustomDeserializer() {
    }

    @Override
    public Object deserializeKey(String key, DeserializationContext ctxt) throws IOException {
        return mapper.readValue(key, GossipMember.class);
    }
}

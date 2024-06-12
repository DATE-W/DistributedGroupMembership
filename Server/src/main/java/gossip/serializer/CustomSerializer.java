package gossip.serializer;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializerProvider;
import gossip.entity.GossipMember;

import java.io.IOException;


/**
 * 自定义序列化
 *
 * @author Pikachudy
 * @date 2022/12/10
 */
public class CustomSerializer extends JsonSerializer<GossipMember> {

    @Override
    public void serialize(GossipMember value, JsonGenerator gen, SerializerProvider serializers) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        gen.writeFieldName(mapper.writeValueAsString(value));
    }
}

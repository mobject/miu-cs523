import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.Map;

public class NeoWSDataDeserializer implements Deserializer {

    @Override
    public void configure(Map configs, boolean isKey) {
    }

    @Override
    public Object deserialize(String topic, byte[] data) {
        ObjectMapper mapper = new ObjectMapper();
        NeoWSData neoWSData = null;
        try {
            neoWSData = mapper.readValue(data, NeoWSData.class);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return neoWSData;
    }

    @Override
    public void close() {

    }
}

package sbp.school.kafka.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import sbp.school.kafka.model.ConfirmMessage;

import java.io.IOException;

import static java.util.Objects.isNull;

/**
 * Kafka JSON Deserializer for ConfirmMessage model
 */
@Slf4j
public class ConfirmMessageKafkaJSONDeserializer implements Deserializer<ConfirmMessage> {

    private final ObjectMapper objectMapper;

    public ConfirmMessageKafkaJSONDeserializer() {
        this.objectMapper = ObjectMapperConfig.getObjectMapperInstance();
    }

    @Override
    public ConfirmMessage deserialize(String topic, byte[] data) {
        if (isNull(data)) return null;

        try {
            return objectMapper.readValue(data, ConfirmMessage.class);
        } catch (IOException e) {
            log.error("Deserialization Error: ", e);
        }
        return null;
    }
}

package sbp.school.kafka.config;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;
import sbp.school.kafka.model.ConfirmMessage;

import static java.util.Objects.isNull;

/**
 * Kafka JSON Serializer for Confirm Message model
 */
@Slf4j
public class ConfirmMessageKafkaJSONSerializer implements Serializer<ConfirmMessage> {

    private final ObjectMapper objectMapper;

    public ConfirmMessageKafkaJSONSerializer() {
        this.objectMapper = ObjectMapperConfig.getObjectMapperInstance();
    }

    @Override
    public byte[] serialize(String topic, ConfirmMessage data) {
        if (isNull(data)) return null;

        try {
            return objectMapper.writeValueAsBytes(data);
        } catch (JsonProcessingException e) {
            log.error("Serialization Error: ", e);
            throw new SerializationException(e);
        }
    }
}

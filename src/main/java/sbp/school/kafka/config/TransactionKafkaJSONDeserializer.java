package sbp.school.kafka.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import sbp.school.kafka.model.Transaction;

import java.io.IOException;

import static java.util.Objects.isNull;

/**
 * Kafka JSON Deserializer for Transaction model
 */
@Slf4j
public class TransactionKafkaJSONDeserializer implements Deserializer<Transaction> {

    private final ObjectMapper objectMapper;

    public TransactionKafkaJSONDeserializer() {
        this.objectMapper = ObjectMapperConfig.getObjectMapperInstance();
    }

    @Override
    public Transaction deserialize(String topic, byte[] data) {
        if (isNull(data)) return null;

        try {
            return objectMapper.readValue(data, Transaction.class);
        } catch (IOException e) {
            log.error("Deserialization Error: ", e);
        }
        return null;
    }
}

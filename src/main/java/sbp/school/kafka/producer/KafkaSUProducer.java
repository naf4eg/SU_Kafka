package sbp.school.kafka.producer;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import sbp.school.kafka.model.Transaction;
import sbp.school.kafka.repository.InMemoryRepository;
import sbp.school.kafka.utils.PropertiesReader;

import java.util.Map;
import java.util.Properties;

/**
 * Kafka S University Producer
 */
@Slf4j
public class KafkaSUProducer {
    private final Properties kafkaProperties;
    private final Map<Long, Transaction> repository = InMemoryRepository.transactionByTimestamp;
    ;

    public KafkaSUProducer() {
        this.kafkaProperties = PropertiesReader.getKafkaSUProducerProperties();
    }

    /**
     * Send Async transaction
     *
     * @param transaction
     */
    public void sendAsync(Transaction transaction) {
        try (var producer = new org.apache.kafka.clients.producer.KafkaProducer<String, Transaction>(kafkaProperties)) {
            var topicName = (String) kafkaProperties.get("topic");

            producer.send(new ProducerRecord<>(topicName, transaction), ((metadata, exception) -> {
                var timestamp = metadata.timestamp();
                repository.put(timestamp, transaction);
            }));

        } catch (Exception e) {
            log.error("", e);
        }
    }
}

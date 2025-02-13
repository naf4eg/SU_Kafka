package sbp.school.kafka.producer;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import sbp.school.kafka.model.ConfirmMessage;
import sbp.school.kafka.model.Transaction;
import sbp.school.kafka.repository.InMemoryRepository;
import sbp.school.kafka.utils.PropertiesReader;

import java.time.LocalDateTime;
import java.util.Map;
import java.util.Properties;

/**
 * Kafka Confirm Message Producer
 */
@Slf4j
public class KafkaConfirmProducer {
    private final Properties kafkaProperties;

    public KafkaConfirmProducer() {
        this.kafkaProperties = PropertiesReader.getKafkaConfirmProducerProperties();
    }

    /**
     * Send Async transaction
     *
     * @param confirmMessage
     */
    public void sendAsync(ConfirmMessage confirmMessage) {
        try (var producer = new org.apache.kafka.clients.producer.KafkaProducer<String, ConfirmMessage>(kafkaProperties)) {
            var topicName = (String) kafkaProperties.get("topic");

            producer.send(new ProducerRecord<>(topicName, confirmMessage), ((metadata, exception) -> {
                log.info("==> Sent new ConfirmMessage: {}", confirmMessage);
            }));
        } catch (Exception e) {
            log.error("", e);
        }
    }
}

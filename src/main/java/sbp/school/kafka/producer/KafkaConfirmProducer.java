package sbp.school.kafka.producer;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.Producer;
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
    private final Producer<String, ConfirmMessage> producer;

    public KafkaConfirmProducer(Producer<String, ConfirmMessage> producer, Properties properties) {
        this.kafkaProperties = properties;
        this.producer = producer;
    }

    /**
     * Send Async transaction
     *
     * @param confirmMessage
     */
    public void sendAsync(ConfirmMessage confirmMessage) {
        try {
            var topicName = (String) kafkaProperties.get("topic");
            producer.send(new ProducerRecord<>(topicName, confirmMessage), ((metadata, exception) -> {
                log.info("==> Sent new ConfirmMessage: {}", confirmMessage);
            }));
        } catch (Exception e) {
            log.error("", e);
        }  finally {
            producer.close();
        }
    }
}

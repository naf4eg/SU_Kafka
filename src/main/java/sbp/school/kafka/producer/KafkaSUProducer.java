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
 * Kafka S University Producer
 */
@Slf4j
public class KafkaSUProducer {
    private final Properties kafkaProperties;
    private final Producer<String, Transaction> producer;
    private static final Map<LocalDateTime, Transaction> dateTimeTransactionMap = InMemoryRepository.PRODUCER_DATE_TIME_TRANSACTION_MAP;

    public KafkaSUProducer(Producer<String, Transaction> producer, Properties properties) {
        this.kafkaProperties = properties;
        this.producer = producer;
    }

    /**
     * Send Async transaction
     *
     * @param transaction
     */
    public void sendAsync(Transaction transaction) {
        try {
            var topicName = (String) kafkaProperties.get("topic");
            producer.send(new ProducerRecord<>(topicName, transaction), ((metadata, exception) -> {
                dateTimeTransactionMap.put(transaction.dateTime(), transaction);
                log.info("==> Sent new Transaction: {}", transaction);
            }));
        } catch (Exception e) {
            log.error("", e);
        } finally {
            producer.close();
        }
    }
}

package sbp.school.kafka.producer;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.ProducerRecord;
import sbp.school.kafka.config.KafkaSUCallback;
import sbp.school.kafka.utils.PropertiesReader;
import sbp.school.kafka.model.Transaction;

import java.util.Properties;

/**
 * Kafka S University Producer
 */
@Slf4j
public class KafkaSUProducer {
    private final Properties kafkaProperties;
    private final Callback callback;

    public KafkaSUProducer() {
        this.kafkaProperties = PropertiesReader.getKafkaProducerProperties();
        this.callback = new KafkaSUCallback();
    }

    /**
     * Send Async transaction
     * @param transaction
     */
    public void sendAsync(Transaction transaction) {
        try (var producer = new org.apache.kafka.clients.producer.KafkaProducer<String, Transaction>(kafkaProperties);) {
            var topicName = (String) kafkaProperties.get("topic");
            producer.send(new ProducerRecord<>(topicName, transaction), callback);
            producer.flush();
        } catch (Exception e) {
            log.error("", e);
        }
    }
}

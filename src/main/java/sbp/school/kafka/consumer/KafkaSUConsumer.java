package sbp.school.kafka.consumer;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import sbp.school.kafka.model.Transaction;
import sbp.school.kafka.repository.InMemoryRepository;
import sbp.school.kafka.utils.PropertiesReader;

import java.time.LocalDateTime;
import java.util.Map;
import java.util.Properties;

/**
 * Kafka S University Consumer
 */
@Slf4j
public class KafkaSUConsumer extends CommonKafkaConsumer<String, Transaction> {

    private static final Map<LocalDateTime, Transaction> consumerDateTimeTransactionMap = InMemoryRepository.CONSUMER_DATE_TIME_TRANSACTION_MAP;

    public KafkaSUConsumer(Consumer<String, Transaction> consumer, Properties properties) {
        super(consumer, properties);
    }

    @Override
    protected void processRecord(ConsumerRecord<String, Transaction> record) {
        log.info("==>> topic={}, partition={}, offset={}, value={}", record.topic(), record.partition(), record.offset(), record.value());
        var transaction = record.value();
        var transactionDateTime = transaction.dateTime();
        consumerDateTimeTransactionMap.put(transactionDateTime, transaction);
        saveOffset(record);
    }
}

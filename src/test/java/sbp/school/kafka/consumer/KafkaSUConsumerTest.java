package sbp.school.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import sbp.school.kafka.model.Transaction;
import sbp.school.kafka.repository.InMemoryRepository;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.*;
import java.util.stream.Collectors;

class KafkaSUConsumerTest {

    private MockConsumer<String, Transaction> consumer;
    private KafkaSUConsumer kafkaSUConsumer;
    private static final Map<LocalDateTime, Transaction> consumerDateTimeTransactionMap = InMemoryRepository.CONSUMER_DATE_TIME_TRANSACTION_MAP;

    private static final String TOPIC = "su_kafka";
    private static final int PARTITION = 0;

    @BeforeEach
    void setUp() {
        consumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
        var properties = new Properties();
        properties.put("topic", TOPIC);
        properties.put("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "sbp.school.kafka.config.TransactionKafkaJSONDeserializer");
        properties.put("group.id", "su-group1");
        kafkaSUConsumer = new KafkaSUConsumer(consumer, properties);
    }

    @Test
    void kafkaSUConsumerTest() {
        consumer.schedulePollTask(() -> {
            consumer.rebalance(Collections.singletonList(new TopicPartition(TOPIC, 0)));
            consumer.addRecord(new ConsumerRecord<>(TOPIC, PARTITION, 0L, "", new Transaction(UUID.randomUUID(), Transaction.Operation.DEBIT, BigDecimal.TEN, "123", LocalDateTime.now(), Transaction.Status.CREATED)));
            consumer.addRecord(new ConsumerRecord<>(TOPIC, PARTITION, 1L, "", new Transaction(UUID.randomUUID(), Transaction.Operation.ARREST, BigDecimal.ZERO, "456", LocalDateTime.now(), Transaction.Status.CREATED)));
        });
        consumer.schedulePollTask(() -> kafkaSUConsumer.stop());

        var startingOffsets = new HashMap<TopicPartition, Long>();
        var topicPartition = new TopicPartition(TOPIC, PARTITION);
        startingOffsets.put(topicPartition,0L);
        consumer.updateBeginningOffsets(startingOffsets);

        kafkaSUConsumer.consume();

        Assertions.assertEquals(2, consumerDateTimeTransactionMap.size());
    }
}
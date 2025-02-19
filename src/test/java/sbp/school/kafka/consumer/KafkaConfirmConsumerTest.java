package sbp.school.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import sbp.school.kafka.model.ConfirmMessage;
import sbp.school.kafka.model.Transaction;
import sbp.school.kafka.repository.InMemoryRepository;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

class KafkaConfirmConsumerTest {

    private MockConsumer<String, ConfirmMessage> consumer;
    private KafkaConfirmConsumer kafkaConfirmConsumer;
    private static final Map<LocalDateTime, Transaction> transactionByDateTime = InMemoryRepository.PRODUCER_DATE_TIME_TRANSACTION_MAP;

    private static final String TOPIC = "confirm_kafka";
    private static final int PARTITION = 0;

    @BeforeEach
    void setUp() {
        consumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
        var properties = new Properties();
        properties.put("topic", TOPIC);
        properties.put("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "sbp.school.kafka.config.ConfirmMessageKafkaJSONDeserializer");
        properties.put("group.id", "confirm-group1");
        kafkaConfirmConsumer = new KafkaConfirmConsumer(consumer, properties);
    }

    @Test
    void kafkaConfirmConsumerTest() {
        var now = LocalDateTime.now();
        transactionByDateTime.put(now, new Transaction(UUID.randomUUID(), Transaction.Operation.DEBIT, BigDecimal.TEN, "123", LocalDateTime.now(), Transaction.Status.CREATED));

        consumer.schedulePollTask(() -> {
            consumer.rebalance(Collections.singletonList(new TopicPartition(TOPIC, 0)));
            consumer.addRecord(new ConsumerRecord<>(TOPIC, PARTITION, 0L, "", new ConfirmMessage(13, now.minusMinutes(3), now.plusMinutes(10))));
        });
        consumer.schedulePollTask(() -> kafkaConfirmConsumer.stop());

        var startingOffsets = new HashMap<TopicPartition, Long>();
        var topicPartition = new TopicPartition(TOPIC, PARTITION);
        startingOffsets.put(topicPartition,0L);
        consumer.updateBeginningOffsets(startingOffsets);

        kafkaConfirmConsumer.consume();

        Assertions.assertEquals(1, transactionByDateTime.size());
        Assertions.assertEquals(Transaction.Status.NOT_CONFIRMED, transactionByDateTime.get(now).status());
    }
}
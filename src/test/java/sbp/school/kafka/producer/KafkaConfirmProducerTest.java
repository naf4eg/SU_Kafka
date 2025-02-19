package sbp.school.kafka.producer;

import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import sbp.school.kafka.config.ConfirmMessageKafkaJSONSerializer;
import sbp.school.kafka.model.ConfirmMessage;
import sbp.school.kafka.model.Transaction;
import sbp.school.kafka.repository.InMemoryRepository;

import java.time.LocalDateTime;
import java.util.Map;
import java.util.Properties;

class KafkaConfirmProducerTest {
    private KafkaConfirmProducer kafkaConfirmProducer;
    private MockProducer<String, ConfirmMessage> mockProducer;
    private static final Map<LocalDateTime, Transaction> transactionByDateTime = InMemoryRepository.PRODUCER_DATE_TIME_TRANSACTION_MAP;



    @BeforeEach
    void setUp() {
        mockProducer = new MockProducer<>(true, new StringSerializer(), new ConfirmMessageKafkaJSONSerializer());
        var properties = new Properties();
        properties.put("topic", "confirm_kafka");
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializerr");
        properties.put("value.serializer", "sbp.school.kafka.config.ConfirmMessageKafkaJSONSerializer");
        properties.put("acks", "all");
        kafkaConfirmProducer = new KafkaConfirmProducer(mockProducer, properties);
        transactionByDateTime.clear();
    }

    @Test
    void sendTransactionProducerTest() {
        var startTime = LocalDateTime.now().minusMinutes(10);
        var endTime = LocalDateTime.now().plusMinutes(10);

        kafkaConfirmProducer.sendAsync(
                new ConfirmMessage(
                        123,
                        startTime,
                        endTime
                )
        );

        var history = mockProducer.history();
        Assertions.assertEquals(1, history.size());
        var actualConfirmMessage = history.getFirst().value();
        Assertions.assertEquals(123, actualConfirmMessage.hash());
        Assertions.assertEquals(startTime, actualConfirmMessage.startDateTime());
        Assertions.assertEquals(endTime, actualConfirmMessage.endDateTime());
    }
}
package sbp.school.kafka.producer;

import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import sbp.school.kafka.config.TransactionKafkaJSONSerializer;
import sbp.school.kafka.model.Transaction;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.Properties;
import java.util.UUID;

class KafkaSUProducerTest {

    private KafkaSUProducer kafkaSUProducer;
    private MockProducer<String, Transaction> mockProducer;


    @BeforeEach
    void setUp() {
        mockProducer = new MockProducer<>(true, new StringSerializer(), new TransactionKafkaJSONSerializer());
        var properties = new Properties();
        properties.put("topic", "su_kafka");
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializerr");
        properties.put("value.serializer", "sbp.school.kafka.config.TransactionKafkaJSONSerializer");
        properties.put("acks", "all");
        kafkaSUProducer = new KafkaSUProducer(mockProducer, properties);
    }

    @Test
    void sendTransactionProducerTest() {
        var now = LocalDateTime.now();
        kafkaSUProducer.sendAsync(new Transaction(
                UUID.randomUUID(),
                Transaction.Operation.DEBIT,
                BigDecimal.TEN,
                "123",
                now,
                Transaction.Status.CREATED
        ));
        var history = mockProducer.history();
        Assertions.assertEquals(1, history.size());
        var actualTransaction = history.getFirst().value();
        Assertions.assertEquals(Transaction.Operation.DEBIT, actualTransaction.operation());
        Assertions.assertEquals(BigDecimal.TEN, actualTransaction.amount());
        Assertions.assertEquals("123", actualTransaction.account());
        Assertions.assertEquals(now, actualTransaction.dateTime());
        Assertions.assertEquals(Transaction.Status.CREATED, actualTransaction.status());
    }
}
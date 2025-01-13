package sbp.school.kafka;

import org.junit.jupiter.api.Test;
import sbp.school.kafka.model.Transaction;
import sbp.school.kafka.producer.KafkaSUProducer;

import java.math.BigDecimal;
import java.time.LocalDate;

public class KafkaSUProduserTest {

    @Test
    void test() {
        var kafkaProducer = new KafkaSUProducer();
        var transaction = new Transaction(
                Transaction.Operation.CREDIT,
                new BigDecimal("33312.34"),
                "43235324234534256",
                LocalDate.now()
        );
        kafkaProducer.sendAsync(transaction);
    }
}

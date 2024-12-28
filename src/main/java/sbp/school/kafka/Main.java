package sbp.school.kafka;

import lombok.extern.slf4j.Slf4j;
import sbp.school.kafka.model.Transaction;
import sbp.school.kafka.producer.KafkaSUProducer;

import java.math.BigDecimal;
import java.time.LocalDate;

@Slf4j
public class Main {
    public static void main(String[] args) {
        var kafkaProducer = new KafkaSUProducer();
        var transaction = new Transaction(
                Transaction.Operation.CREDIT,
                new BigDecimal("10.34"),
                "43235324234534256",
                LocalDate.now()
        );
        kafkaProducer.sendAsync(transaction);
    }
}

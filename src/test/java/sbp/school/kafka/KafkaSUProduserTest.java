package sbp.school.kafka;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import sbp.school.kafka.model.Transaction;
import sbp.school.kafka.producer.KafkaSUProducer;
import sbp.school.kafka.repository.InMemoryRepository;
import sbp.school.kafka.scheduler.RetryerTransactionScheduler;

import java.math.BigDecimal;
import java.time.LocalDateTime;

@Slf4j
public class KafkaSUProduserTest {

    @Test
    void testCreated() {
        var kafkaProducer = new KafkaSUProducer();
        var transaction = new Transaction(
                Transaction.Operation.CREDIT,
                new BigDecimal("33312.34"),
                "43235324234534256",
                LocalDateTime.now(),
                Transaction.Status.CREATED
        );
        kafkaProducer.sendAsync(transaction);
    }

    @Test
    @SneakyThrows
    void testNotConfirmed() {
        var kafkaProducer = new KafkaSUProducer();
        var transaction = new Transaction(
                Transaction.Operation.CREDIT,
                new BigDecimal("33312.34"),
                "43235324234534256",
                LocalDateTime.now(),
                Transaction.Status.NOT_CONFIRMED
        );
        kafkaProducer.sendAsync(transaction);
        new RetryerTransactionScheduler().retry();

        while (true) {
            var dateTimeTransactionMap = InMemoryRepository.PRODUCER_DATE_TIME_TRANSACTION_MAP;
            Thread.sleep(1000);
            log.info("{}", dateTimeTransactionMap);
        }
    }
}

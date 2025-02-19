package sbp.school.kafka.scheduler;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import sbp.school.kafka.model.Transaction;
import sbp.school.kafka.producer.KafkaSUProducer;
import sbp.school.kafka.repository.InMemoryRepository;
import sbp.school.kafka.utils.PropertiesReader;

import java.time.LocalDateTime;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

@Slf4j
public class RetryerTransactionScheduler {
    public static final int PERIOD = 10;
    public static final int INITIAL_DELAY = 10;
    private final ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
    private static final Map<LocalDateTime, Transaction> transactionByTimestamp = InMemoryRepository.PRODUCER_DATE_TIME_TRANSACTION_MAP;
    private final KafkaSUProducer kafkaSUProducer;

    public RetryerTransactionScheduler() {
        this.kafkaSUProducer = new KafkaSUProducer(new KafkaProducer<>(PropertiesReader.getKafkaSUProducerProperties()), PropertiesReader.getKafkaConfirmConsumerProperties());
    }

    public void retry() {
        scheduledExecutorService.scheduleAtFixedRate(
                this::process,
                INITIAL_DELAY,
                PERIOD,
                TimeUnit.SECONDS
        );
    }

    private void process() {
        log.info("==> Start retry process");

        var transactions = transactionByTimestamp.values()
                .stream()
                .filter(transaction -> Transaction.Status.NOT_CONFIRMED == transaction.status())
                .toList();

        transactions.forEach(transaction -> {
            transactionByTimestamp.remove(transaction.dateTime());
            var newTransaction = transaction.withCreatedStatus();
            kafkaSUProducer.sendAsync(newTransaction);
            log.info("==> Retry process sent transaction: {}", newTransaction);
        });

        log.info("==> Finish retry process");
    }
}

package sbp.school.kafka.scheduler;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import sbp.school.kafka.model.ConfirmMessage;
import sbp.school.kafka.model.Transaction;
import sbp.school.kafka.producer.KafkaConfirmProducer;
import sbp.school.kafka.repository.InMemoryRepository;
import sbp.school.kafka.utils.PropertiesReader;

import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

@Slf4j
public class ConfirmMessageScheduler {
    public static final int PERIOD = 60;
    public static final int INITIAL_DELAY = 60;
    private final ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
    private static final Map<LocalDateTime, Transaction> CONSUMER_DATE_TIME_TRANSACTION_MAP = InMemoryRepository.CONSUMER_DATE_TIME_TRANSACTION_MAP;
    private final KafkaConfirmProducer kafkaConfirmProducer;
    private final Properties properties;

    public ConfirmMessageScheduler() {
        this.kafkaConfirmProducer = new KafkaConfirmProducer(new KafkaProducer<>(PropertiesReader.getKafkaConfirmConsumerProperties()), PropertiesReader.getKafkaConfirmConsumerProperties());
        this.properties = PropertiesReader.getAppProperties();
    }

    public void confirm() {
        scheduledExecutorService.scheduleAtFixedRate(
                this::process,
                INITIAL_DELAY,
                PERIOD,
                TimeUnit.SECONDS
        );
    }

    private void process() {
        log.info("==> Start confirm process");

        var lagTime = (Long) properties.get("confirm.lag.time");

        var transactionLast = CONSUMER_DATE_TIME_TRANSACTION_MAP
                .values()
                .stream()
                .max(Comparator.comparing(Transaction::dateTime));

        if (transactionLast.isEmpty()) {
            return;
        }

        var lagLastDt = transactionLast.get().dateTime().minusSeconds(lagTime);

        var transactionList = CONSUMER_DATE_TIME_TRANSACTION_MAP
                .values()
                .stream()
                .filter(transaction -> transaction.dateTime().isBefore(lagLastDt) || transaction.dateTime().isEqual(lagLastDt))
                .toList();


        var startDt = transactionList.getFirst().dateTime();
        var endDt = transactionList.getLast().dateTime();
        var hashCode = transactionList.stream().map(Transaction::id)
                .map(UUID::toString)
                .collect(Collectors.joining())
                .hashCode();

        var confirmMessage = new ConfirmMessage(hashCode, startDt, endDt);

        kafkaConfirmProducer.sendAsync(confirmMessage);

        transactionList.forEach(transaction -> CONSUMER_DATE_TIME_TRANSACTION_MAP.remove(transaction.dateTime()));

        log.info("==> Finish confirm process");
    }
}

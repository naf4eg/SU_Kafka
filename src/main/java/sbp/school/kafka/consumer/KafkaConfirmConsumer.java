package sbp.school.kafka.consumer;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import sbp.school.kafka.model.ConfirmMessage;
import sbp.school.kafka.model.Transaction;
import sbp.school.kafka.repository.InMemoryRepository;
import sbp.school.kafka.utils.PropertiesReader;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

/**
 * фф
 * Kafka S University Consumer
 */
@Slf4j
public class KafkaConfirmConsumer extends CommonKafkaConsumer<String, ConfirmMessage> {

    private static final Map<LocalDateTime, Transaction> transactionByDateTime = InMemoryRepository.PRODUCER_DATE_TIME_TRANSACTION_MAP;

    public KafkaConfirmConsumer() {
        super(PropertiesReader.getKafkaConfirmConsumerProperties());
    }


    @Override
    protected void processRecord(ConsumerRecord<String, ConfirmMessage> record) {
        log.info("==>> topic={}, partition={}, offset={}, value={}, timestamp={}",
                record.topic(),
                record.partition(),
                record.offset(),
                record.value(),
                record.timestamp()
        );

        var confirmMessage = record.value();
        var startDateTime = confirmMessage.startDateTime();
        var endDateTime = confirmMessage.endDateTime();
        var hash = confirmMessage.hash();

        var ids = new ArrayList<String>();
        var dateTimes = new ArrayList<LocalDateTime>();
        transactionByDateTime.forEach((dateTime, transaction) -> {
            if ((dateTime.isBefore(startDateTime) && dateTime.isAfter(endDateTime)) || (dateTime.isEqual(startDateTime) || dateTime.isEqual(endDateTime))) {
                ids.add(transaction.id().toString());
                dateTimes.add(dateTime);
            }
        });
        var resultHashCode = String.join("", ids).hashCode();

        if (hash == resultHashCode) {
            updateTransactionsStatus(dateTimes, Transaction::withConfirmedStatus);
        } else {
            updateTransactionsStatus(dateTimes, Transaction::withNotConfirmedStatus);
        }

        saveOffset(record);
    }

    private void updateTransactionsStatus(List<LocalDateTime> dateTimeList, Function<Transaction, Transaction> newTransaction) {
        transactionByDateTime.replaceAll(
                (dateTime, transaction) -> {
                    if (dateTimeList.contains(dateTime)) {
                        return newTransaction.apply(transaction);
                    }
                    return transaction;
                }
        );

    }
}

package sbp.school.kafka.consumer;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import sbp.school.kafka.model.ConfirmMessage;
import sbp.school.kafka.model.Transaction;
import sbp.school.kafka.repository.InMemoryRepository;
import sbp.school.kafka.utils.PropertiesReader;

import java.time.Duration;
import java.util.*;
import java.util.stream.Collectors;

import static java.util.Objects.nonNull;

/**
 * Kafka S University Consumer
 */
@Slf4j
public class KafkaConfirmConsumer {
    private final Properties properties;
    private final KafkaConsumer<String, ConfirmMessage> consumer;
    private final List<String> topics;
    private final Map<Long, Transaction> repository = InMemoryRepository.transactionByTimestamp;


    private final Map<TopicPartition, OffsetAndMetadata> currentOffsets = new HashMap<>();

    public KafkaConfirmConsumer() {
        this.properties = PropertiesReader.getKafkaConfirmConsumerProperties();
        this.consumer = new KafkaConsumer<>(this.properties);
        this.topics = List.of(properties.getProperty("topic"));
    }

    public void consume() {
        try {
            consumer.subscribe(topics);
            consumer.poll(Duration.ofMillis(0));
            consumer.assignment().forEach(this::initConsumerOffset);
            while (true) {
                consumer.poll(Duration.ofMillis(10000))
                        .forEach(this::processRecord);
                consumer.commitAsync(currentOffsets, null);
            }
        } catch (Exception e) {
            log.error("Unexpected error", e);
        } finally {
            try {
                consumer.commitSync();
            } finally {
                consumer.close();
            }
        }
    }

    private void processRecord(ConsumerRecord<String, ConfirmMessage> record) {
        log.info("==>> topic={}, partition={}, offset={}, value={}, timestamp={}",
                record.topic(),
                record.partition(),
                record.offset(),
                record.value(),
                record.timestamp()
        );

        var confirmMessage = record.value();
        var startTimestamp = confirmMessage.startTimestamp();
        var endTimestamp = confirmMessage.endTimestamp();
        var hash = confirmMessage.hash();

        var ids = new ArrayList<String>();
        var timestamps = new ArrayList<Long>();
        repository.forEach((k, v) -> {
            if (k >= startTimestamp && k <= endTimestamp) {
                ids.add(v.id().toString());
                timestamps.add(k);
            }
        });
        var resultHashCode = String.join("", ids).hashCode();
        if (hash == resultHashCode) {
            timestamps.forEach(repository::remove);
        }

        saveOffset(record);
    }

    private void initConsumerOffset(TopicPartition partition) {
        var offsetAndMetadata = getOffsetAndMetadataByPartition(partition);
        if (nonNull(offsetAndMetadata)) {
            consumer.seek(partition, offsetAndMetadata);
        }
    }

    private void saveOffset(ConsumerRecord<String, ConfirmMessage> record) {
        currentOffsets.put(
                new TopicPartition(record.topic(), record.partition()),
                new OffsetAndMetadata(record.offset() + 1L, "no metadata")
        );
    }

    private OffsetAndMetadata getOffsetAndMetadataByPartition(TopicPartition partition) {
        return currentOffsets.get(partition);
    }
}

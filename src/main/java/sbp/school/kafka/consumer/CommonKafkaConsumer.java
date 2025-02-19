package sbp.school.kafka.consumer;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import sbp.school.kafka.model.Transaction;

import java.time.Duration;
import java.util.*;

import static java.util.Objects.nonNull;

@Slf4j
public abstract class CommonKafkaConsumer<K, V> {

    protected final Consumer<K, V> consumer;
    private final List<String> topics;


    private final Map<TopicPartition, OffsetAndMetadata> currentOffsets = new HashMap<>();

    protected CommonKafkaConsumer(Consumer<K,V> consumer, Properties properties) {
        this.consumer = consumer;
        this.topics = List.of(properties.getProperty("topic"));
    }

    public void consume() {
        try {
            consumer.subscribe(topics, getConsumerRebalanceListener());
//            consumer.poll(Duration.ofMillis(0));
//            consumer.assignment().forEach(this::initConsumerOffset);
            while (true) {
                consumer.poll(Duration.ofMillis(1000))
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

    protected abstract void processRecord(ConsumerRecord<K, V> kvConsumerRecord);

    protected void initConsumerOffset(TopicPartition partition) {
        var offsetAndMetadata = getOffsetAndMetadataByPartition(partition);
        if (nonNull(offsetAndMetadata)) {
            consumer.seek(partition, offsetAndMetadata);
        }
    }

    protected void saveOffset(ConsumerRecord<K, V> record) {
        currentOffsets.put(
                new TopicPartition(record.topic(), record.partition()),
                new OffsetAndMetadata(record.offset() + 1L, "no metadata")
        );
    }

    private OffsetAndMetadata getOffsetAndMetadataByPartition(TopicPartition partition) {
        return currentOffsets.get(partition);
    }

    private ConsumerRebalanceListener getConsumerRebalanceListener() {
        return new ConsumerRebalanceListener() {

            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {

            }

            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                partitions.forEach(partition -> initConsumerOffset(partition));
            }
        };
    }

    public void stop() {
        consumer.wakeup();
    }
}

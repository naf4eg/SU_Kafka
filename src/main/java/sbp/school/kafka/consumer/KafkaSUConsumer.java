package sbp.school.kafka.consumer;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import sbp.school.kafka.utils.PropertiesReader;

/**
 * Kafka S University Consumer
 */
@Slf4j
public class KafkaSUConsumer extends CommonKafkaConsumer<String, String> {

    public KafkaSUConsumer() {
        super(PropertiesReader.getKafkaSUConsumerProperties());
    }

    @Override
    protected void processRecord(ConsumerRecord<String, String> record) {
        log.info("==>> topic={}, partition={}, offset={}, value={}", record.topic(), record.partition(), record.offset(), record.value());
        saveOffset(record);
    }
}

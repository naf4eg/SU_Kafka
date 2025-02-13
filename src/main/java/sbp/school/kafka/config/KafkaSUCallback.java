package sbp.school.kafka.config;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;

import static java.util.Objects.nonNull;

/**
 * Kafka S University Callback
 */
@Slf4j
public class KafkaSUCallback implements Callback {

    @Override
    public void onCompletion(RecordMetadata metadata, Exception exception) {
        var offset = metadata.offset();
        var partition = metadata.partition();
        var topic = metadata.topic();

        if (nonNull(exception)) {
            log.error("Message not sent to topic={}, partition={}, offset={}", topic, partition, offset);
            log.error("", exception);
        }

        log.debug("Message sent to topic={}, partition={}, offset={}", topic, partition, offset);
    }
}

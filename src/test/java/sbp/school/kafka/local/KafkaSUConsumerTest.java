package sbp.school.kafka.local;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import sbp.school.kafka.consumer.KafkaSUConsumer;
import sbp.school.kafka.utils.PropertiesReader;

class KafkaSUConsumerTest {

    @Test
    @Disabled
    public void consumerTest() {
        var kafkaSUConsumer = new KafkaSUConsumer(new KafkaConsumer<>(PropertiesReader.getKafkaSUConsumerProperties()), PropertiesReader.getKafkaSUConsumerProperties());
        kafkaSUConsumer.consume();
    }
}
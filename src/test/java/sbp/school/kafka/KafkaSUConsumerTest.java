package sbp.school.kafka;

import org.junit.jupiter.api.Test;
import sbp.school.kafka.consumer.KafkaSUConsumer;

class KafkaSUConsumerTest {

    @Test
    public void consumerTest() {
        var kafkaSUConsumer = new KafkaSUConsumer();
        kafkaSUConsumer.consume();
    }
}
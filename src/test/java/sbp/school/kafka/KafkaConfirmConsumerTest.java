package sbp.school.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.SneakyThrows;
import org.junit.jupiter.api.Test;
import sbp.school.kafka.consumer.KafkaConfirmConsumer;
import sbp.school.kafka.consumer.KafkaSUConsumer;
import sbp.school.kafka.model.ConfirmMessage;

class KafkaConfirmConsumerTest {

    @Test
    public void consumerTest() {
        var kafkaConfirmConsumer = new KafkaConfirmConsumer();
        kafkaConfirmConsumer.consume();
    }

    @Test
    @SneakyThrows
    void msgGenerator() {
        var objectMapper = new ObjectMapper();
        var s = objectMapper.writeValueAsString(new ConfirmMessage(23, 1234567890L, 22234567890L));
        System.out.println(s);
    }
}
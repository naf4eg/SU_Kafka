package sbp.school.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.SneakyThrows;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import sbp.school.kafka.consumer.KafkaConfirmConsumer;
import sbp.school.kafka.consumer.KafkaSUConsumer;
import sbp.school.kafka.model.ConfirmMessage;

import java.time.LocalDateTime;

class KafkaConfirmConsumerTest {

    @Test
    public void consumerTest() {
        var kafkaConfirmConsumer = new KafkaConfirmConsumer();
        kafkaConfirmConsumer.consume();
    }

    @Test
    @SneakyThrows
    @Disabled
    void msgGenerator() {
        var objectMapper = new ObjectMapper();
        var s = objectMapper.writeValueAsString(new ConfirmMessage(23, LocalDateTime.now(), LocalDateTime.now()));
        System.out.println(s);
    }
}
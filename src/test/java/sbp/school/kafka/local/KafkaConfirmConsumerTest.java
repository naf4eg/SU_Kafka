package sbp.school.kafka.local;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.SneakyThrows;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import sbp.school.kafka.consumer.KafkaConfirmConsumer;
import sbp.school.kafka.model.ConfirmMessage;
import sbp.school.kafka.utils.PropertiesReader;

import java.time.LocalDateTime;

class KafkaConfirmConsumerTest {

    @Test
    @Disabled
    public void consumerTest() {
        var kafkaConfirmConsumer = new KafkaConfirmConsumer(new KafkaConsumer<>(PropertiesReader.getKafkaConfirmConsumerProperties()),PropertiesReader.getKafkaConfirmConsumerProperties());
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
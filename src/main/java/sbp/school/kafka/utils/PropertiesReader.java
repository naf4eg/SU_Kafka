package sbp.school.kafka.utils;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.Properties;

/**
 * Properties reader from .properties file
 */
@Slf4j
public class PropertiesReader {

    public static Properties getKafkaSUProducerProperties() {
        return readPropertiesByFileName("kafkaSUProducer.properties");
    }

    public static Properties getKafkaSUConsumerProperties() {
        return readPropertiesByFileName("kafkaSUConsumer.properties");
    }

    public static Properties getKafkaConfirmConsumerProperties() {
        return readPropertiesByFileName("kafkaConfirmConsumer.properties");
    }

    public static Properties getKafkaConfirmProducerProperties() {
        return readPropertiesByFileName("kafkaConfirmProducer.properties");
    }

    public static Properties getAppProperties() {
        return readPropertiesByFileName("app.properties");
    }

    private static Properties readPropertiesByFileName(String name) {
        try (var resourcesStream = Thread.currentThread().getContextClassLoader().getResourceAsStream(name)) {
            var properties = new Properties();
            properties.load(resourcesStream);
            return properties;
        } catch (IOException e) {
            log.error("Error read properties: ", e);
            throw new RuntimeException(e);
        }
    }
}

package sbp.school.kafka.utils;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.networknt.schema.JsonSchema;
import com.networknt.schema.JsonSchemaFactory;
import com.networknt.schema.SpecVersion;
import com.networknt.schema.ValidationMessage;
import lombok.extern.slf4j.Slf4j;
import sbp.school.kafka.config.ObjectMapperConfig;

import java.util.Set;

/**
 * JSON Schema Validator
 */
@Slf4j
public class JSONSchemaValidator {
    private static final ObjectMapper objectMapper = ObjectMapperConfig.getObjectMapperInstance();
    private static final JsonSchemaFactory factory = JsonSchemaFactory.getInstance(SpecVersion.VersionFlag.V4);

    public static <T> boolean isValid(T object, String pathToJsonSchema) {
        JsonNode json = objectMapper.valueToTree(object);
        JsonSchema schema = factory.getSchema(JSONSchemaValidator.class.getResourceAsStream(pathToJsonSchema));
        Set<ValidationMessage> validationMessages = schema.validate(json);
        if (!validationMessages.isEmpty()) {
            validationMessages.forEach(validationMessage -> log.warn("Validation error {}", validationMessage));
        }
        return validationMessages.isEmpty();
    }
}
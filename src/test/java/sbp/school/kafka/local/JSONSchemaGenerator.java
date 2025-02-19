package sbp.school.kafka.local;

import com.github.victools.jsonschema.generator.OptionPreset;
import com.github.victools.jsonschema.generator.SchemaGenerator;
import com.github.victools.jsonschema.generator.SchemaGeneratorConfigBuilder;
import com.github.victools.jsonschema.generator.SchemaVersion;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import sbp.school.kafka.model.Transaction;

@Slf4j
public class JSONSchemaGenerator {

    @Test
    @Disabled
    public void generateJSONSchema() {
        var schemaGeneratorConfigBuilder = new SchemaGeneratorConfigBuilder(SchemaVersion.DRAFT_7, OptionPreset.PLAIN_JSON);
        var schemaGenerator = new SchemaGenerator(schemaGeneratorConfigBuilder.build());
        var schema = schemaGenerator.generateSchema(Transaction.class);
        log.info("Generated json schema by Transaction.class: \n {}", schema.toPrettyString());
    }
}

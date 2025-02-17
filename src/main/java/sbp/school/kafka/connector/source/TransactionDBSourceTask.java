package sbp.school.kafka.connector.source;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;

import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

@Slf4j
public class TransactionDBSourceTask extends SourceTask {

    public static final String POSITION_FIELD = "position";
    private static final Schema VALUE_SCHEMA = Schema.STRING_SCHEMA;

    private String dbUrl;
    private String dbUser;
    private String dbPassword;
    private String topic;
    private Long dbOffset = 0L;

    @Override
    public String version() {
        return new TransactionDBSourceConnector().version();
    }

    @Override
    public void start(Map<String, String> props) {
        AbstractConfig config = new AbstractConfig(TransactionDBSourceConnector.CONFIG_DEF, props);
        dbUrl = config.getString(TransactionDBSourceConnector.DB_URL);
        dbUser = config.getString(TransactionDBSourceConnector.DB_USER);
        dbPassword = config.getString(TransactionDBSourceConnector.DB_PASSWORD);
        topic = config.getString(TransactionDBSourceConnector.TOPIC_CONFIG);
    }

    @Override
    public List<SourceRecord> poll() {

        List<String> transactions = getTransactions();
        return transactions
                .stream()
                .map(this::createSourceRecord)
                .toList();
    }

    @Override
    public void stop() {

    }

    private SourceRecord createSourceRecord(String transaction) {
        var sourceRecord = new SourceRecord(
                offsetKey(this.dbUrl),
                offsetValue(this.dbOffset),
                this.topic,
                null,
                null,
                null,
                VALUE_SCHEMA,
                transaction,
                System.currentTimeMillis()
        );
        log.info("==>> sourceRecord {}", sourceRecord);
        return sourceRecord;
    }

    private List<String> getTransactions() {
        List<String> result = new ArrayList<>();

        try (var connection = DriverManager.getConnection(dbUrl, dbUser, dbPassword);
             PreparedStatement preparedStatement = connection.prepareStatement("select * from transaction")) {
            ResultSet resultSet = preparedStatement.executeQuery();
            while (resultSet.next()) {
                result.add("id = " + resultSet.getString("id") +
                        " name = " + resultSet.getString("name")
                );
            }
        } catch (Exception e) {
            log.error("", e);
            throw new RuntimeException(e);
        }

        return result;
    }

    private Map<String, String> offsetKey(String filename) {
        return Collections.singletonMap(TransactionDBSourceConnector.DB_URL, filename);
    }

    private Map<String, Long> offsetValue(Long pos) {
        return Collections.singletonMap(POSITION_FIELD, pos);
    }
}

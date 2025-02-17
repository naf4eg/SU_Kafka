package sbp.school.kafka.connector.source;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.source.SourceConnector;
import sbp.school.kafka.utils.PropertiesReader;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Slf4j
public class TransactionDBSourceConnector  extends SourceConnector {

    public static final String DB_URL = "h2.db.url";
    public static final String DB_USER = "h2.db.username";
    public static final String DB_PASSWORD = "h2.db.password";
    public static final String TOPIC_CONFIG = "topic";

    static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(DB_URL, Type.STRING, PropertiesReader.getKafkaConnectorsProperties().getProperty(DB_URL), ConfigDef.Importance.HIGH, "URL DB")
            .define(DB_USER, Type.STRING, PropertiesReader.getKafkaConnectorsProperties().getProperty(DB_USER), ConfigDef.Importance.HIGH, "USERNAME DB")
            .define(DB_PASSWORD, Type.STRING, PropertiesReader.getKafkaConnectorsProperties().getProperty(DB_PASSWORD), ConfigDef.Importance.HIGH, "USER PASSWORD DB")
            .define(TOPIC_CONFIG, Type.STRING, PropertiesReader.getKafkaConnectorsProperties().getProperty(TOPIC_CONFIG), new ConfigDef.NonEmptyString(), ConfigDef.Importance.HIGH, "The topic to publish data to");

    private Map<String, String> props;

    @Override
    public String version() {
        return AppInfoParser.getVersion();
    }

    @Override
    public void start(Map<String, String> props) {
        this.props = props;
        AbstractConfig config = new AbstractConfig(CONFIG_DEF, props);
        String dbURL = config.getString(DB_URL);
        String dbUser = config.getString(DB_USER);
        String dbPassword = config.getString(DB_PASSWORD);
        log.info("==> Start reading from H2DB with dbURL:{}, dbUser:{}, dbPassword:{}", dbURL, dbUser, dbPassword);
    }

    @Override
    public Class<? extends Task> taskClass() {
        return TransactionDBSourceTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        List<Map<String, String>> configs = new ArrayList<>();
        configs.add(props);
        return configs;
    }

    @Override
    public void stop() {
    }

    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }
}

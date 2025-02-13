package sbp.school.kafka.config;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.InvalidRecordException;
import sbp.school.kafka.model.Transaction;

import java.util.Map;

/**
 * Kafka partitioner for Transaction model.
 * Partitioning is carried out by Transaction Operation Type
 */
@Slf4j
public class TransactionPartitioner implements Partitioner {
    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        var partitionInfos = cluster.partitionsForTopic(topic);
        var numPartitions = partitionInfos.size();
        var lastPartition = numPartitions - 1;

        if (!(value instanceof Transaction)) {
            throw new InvalidRecordException("Value is not Transaction object");
        }

        var hash = ((Transaction) value).operation().ordinal();
        if (hash >= numPartitions) {
            return lastPartition;
        }

        return hash % numPartitions;
    }

    @Override
    public void close() {
        log.info("Partition is closed");
    }

    @Override
    public void configure(Map<String, ?> configs) {

    }
}

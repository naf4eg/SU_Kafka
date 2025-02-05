package sbp.school.kafka.repository;

import lombok.extern.slf4j.Slf4j;
import sbp.school.kafka.model.Transaction;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
public class InMemoryRepository {
    public static final Map<Long, Transaction> transactionByTimestamp = new ConcurrentHashMap<>();
}

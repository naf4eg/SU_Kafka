package sbp.school.kafka.repository;

import lombok.extern.slf4j.Slf4j;
import sbp.school.kafka.model.Transaction;

import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
public class InMemoryRepository {
    public static final Map<LocalDateTime, Transaction> DATE_TIME_TRANSACTION_MAP = new ConcurrentHashMap<>();
}

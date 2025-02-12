package sbp.school.kafka.model;

import sbp.school.kafka.utils.JSONSchemaValidator;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.UUID;

/**
 * Transaction model
 *
 * @param id        transaction
 * @param operation type transaction
 * @param amount    transaction
 * @param account   transaction
 * @param dateTime  transaction
 */
public record Transaction(
        UUID id,
        Operation operation,
        BigDecimal amount,
        String account,
        LocalDateTime dateTime,
        Status status
) {
    public Transaction(Operation operation, BigDecimal amount, String account, LocalDateTime dateTime, Status status) {
        this(UUID.randomUUID(), operation, amount, account, dateTime, status);
        this.validateByJsonSchema();
    }

    /**
     * Operation Type
     */
    public enum Operation {
        DEBIT, CREDIT, BLOCK, ARREST
    }

    public enum Status {
        CREATED, CONFIRMED, NOT_CONFIRMED
    }

    public Transaction withConfirmedStatus() {
        return new Transaction(id, operation, amount, account, dateTime, Status.CONFIRMED);
    }

    public Transaction withNotConfirmedStatus() {
        return new Transaction(id, operation, amount, account, dateTime, Status.NOT_CONFIRMED);
    }

    public Transaction withCreatedStatus() {
        return new Transaction(id, operation, amount, account, LocalDateTime.now(), Status.CREATED);
    }

    private void validateByJsonSchema() {
        var isValid = JSONSchemaValidator.isValid(this, "/TransactionJSONSchema.json");
        if (!isValid) {
            throw new RuntimeException("Object " + id + " is not Valid");
        }
    }
}

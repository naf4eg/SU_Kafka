package sbp.school.kafka.model;

import sbp.school.kafka.utils.JSONSchemaValidator;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.util.UUID;

/**
 * Transaction model
 * @param id transaction
 * @param operation type transaction
 * @param amount transaction
 * @param account transaction
 * @param date transaction
 */
public record Transaction(
        UUID id,
        Operation operation,
        BigDecimal amount,
        String account,
        LocalDate date
) {
    public Transaction(Operation operation, BigDecimal amount, String account, LocalDate date) {
        this(UUID.randomUUID(), operation, amount, account, date);
        this.validateByJsonSchema();
    }

    /**
     * Operation Type
     */
    public enum Operation {
        DEBIT, CREDIT, BLOCK, ARREST
    }

    private void validateByJsonSchema() {
        var isValid = JSONSchemaValidator.isValid(this, "/TransactionJSONSchema.json");
        if (!isValid) {
            throw new RuntimeException("Object " + id + " is not Valid");
        }
    }
}

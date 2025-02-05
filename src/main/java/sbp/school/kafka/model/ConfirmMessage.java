package sbp.school.kafka.model;

public record ConfirmMessage(Integer hash, Long startTimestamp, Long endTimestamp) {
}

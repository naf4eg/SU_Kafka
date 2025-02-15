package sbp.school.kafka.model;

import java.time.LocalDateTime;

public record ConfirmMessage(Integer hash, LocalDateTime startDateTime, LocalDateTime endDateTime) {
}

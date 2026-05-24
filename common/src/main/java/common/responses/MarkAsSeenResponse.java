package common.responses;

import java.io.Serializable;
import java.time.LocalDateTime;

public record MarkAsSeenResponse(long messageId, LocalDateTime seenTime) implements Serializable {
}

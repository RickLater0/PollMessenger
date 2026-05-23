package common.requests;

import java.io.Serializable;

public record MarkAsSeenRequest(long messageId) implements Serializable {
}

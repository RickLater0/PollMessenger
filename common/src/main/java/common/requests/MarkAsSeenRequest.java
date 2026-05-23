package common.requests;

import common.dto.User;

import java.io.Serializable;

public record MarkAsSeenRequest(long messageId) implements Serializable {
}

package common.responses;

import common.dto.User;

public record MarkAsSeenResponse(User from, User whoSeen, long messageId) {

}

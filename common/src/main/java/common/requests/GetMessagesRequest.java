package common.requests;

import common.dto.MessageSeenState;
import common.dto.User;

import java.io.Serializable;

public record GetMessagesRequest(User from, MessageSeenState state) implements Serializable {
}

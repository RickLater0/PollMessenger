package common.requests;

import common.dto.MessageContent;
import common.dto.User;

import java.io.Serializable;
import java.time.LocalDateTime;

public record SendMessageRequest(MessageContent content, User to) implements Serializable {
}

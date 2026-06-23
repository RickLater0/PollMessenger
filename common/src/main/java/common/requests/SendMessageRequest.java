package common.requests;

import common.dto.MessageContent;
import common.dto.User;

import java.io.Serializable;

public record SendMessageRequest(MessageContent content, String to) implements Serializable {
}

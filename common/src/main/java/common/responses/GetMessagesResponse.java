package common.responses;

import common.dto.Message;

import java.io.Serializable;
import java.util.List;

public record GetMessagesResponse(List<Message> messages) implements Serializable {
}

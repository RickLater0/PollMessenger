package common.requests;

import common.dto.MessageSeenState;

import java.io.Serializable;



public record GetAllMessagesRequest(MessageSeenState state) implements Serializable {
}

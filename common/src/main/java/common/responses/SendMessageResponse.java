package common.responses;

import java.io.Serializable;

public record SendMessageResponse(Integer messageId) implements Serializable {
}

package common.responses;

import java.io.Serializable;

public record SendMessageResponse(long messageId) implements Serializable {
}

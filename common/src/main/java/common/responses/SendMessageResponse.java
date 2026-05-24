package common.responses;

import java.io.Serializable;

public record SendMessageResponse(int messageId) implements Serializable {
}

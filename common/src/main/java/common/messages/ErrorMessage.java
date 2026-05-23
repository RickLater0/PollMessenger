package common.messages;

import java.io.Serializable;

public record ErrorMessage(String message) implements Serializable {
}

package common.requests;

import java.io.Serializable;

public record AuthPollRequest(String name, String passwd) implements Serializable {
}

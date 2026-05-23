package common.requests;

import common.dto.User;
import java.io.Serializable;

public record PollRequest(User user) implements Serializable {
}
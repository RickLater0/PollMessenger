package common.responses;

import java.io.Serializable;
import java.util.List;

public record GetActiveUsersResponse(List<String> usernames) implements Serializable {
}

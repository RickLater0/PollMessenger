package common.responses;

import java.io.Serializable;

public record ClientLogoutResponse(String username) implements Serializable {
}

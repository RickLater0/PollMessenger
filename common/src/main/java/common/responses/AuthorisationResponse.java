package common.responses;

import java.io.Serializable;

public record AuthorisationResponse(boolean success) implements Serializable {
}

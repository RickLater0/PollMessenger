package common.requests;

import java.io.Serializable;

public record AuthorisationRequest(String name, String passwd) implements Serializable {
}

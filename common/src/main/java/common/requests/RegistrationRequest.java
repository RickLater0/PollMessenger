package common.requests;

import java.io.Serializable;

public record RegistrationRequest(String name, String passwd) implements Serializable {
}

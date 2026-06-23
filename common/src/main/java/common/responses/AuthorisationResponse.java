package common.responses;

import java.io.Serializable;
//id == -1 значит авторизация не прошла
public record AuthorisationResponse(Integer id) implements Serializable {
}

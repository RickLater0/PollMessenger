package common.requests;

import common.dto.User;
import java.io.Serializable;

public record GetDialogRequest(User withUser) implements Serializable {
}
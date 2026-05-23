package common.responses;

import common.dto.User;

import java.io.Serializable;
import java.util.List;

public record GetNamesResponse(List<User> users) implements Serializable {
}

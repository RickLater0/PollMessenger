package common.responses;

import java.io.Serializable;
import java.util.Map;

public record GetNamesResponse(Map<String, Boolean> users) implements Serializable {
}

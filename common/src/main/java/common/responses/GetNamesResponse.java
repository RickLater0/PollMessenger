package common.responses;

import java.io.Serializable;
import java.util.List;

public record GetNamesResponse(List<String> users) implements Serializable {
}

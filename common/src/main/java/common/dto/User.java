package common.dto;

import java.io.Serializable;

public record User(Integer id, String name) implements Serializable {
}

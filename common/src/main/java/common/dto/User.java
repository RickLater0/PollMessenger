package common.dto;

import java.io.Serializable;

public record User(Long id, String name) implements Serializable {
}

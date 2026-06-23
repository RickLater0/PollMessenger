package common.requests;

import java.io.Serializable;

public record GetDialogRequest(String withUser) implements Serializable {
}
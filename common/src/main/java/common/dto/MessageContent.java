package common.dto;

import java.io.Serializable;

//можно добавить поддержку разных типов сообщений... в консоли ага. PNG->ASCII графика, поехали
public record MessageContent(String content) implements Serializable {

}

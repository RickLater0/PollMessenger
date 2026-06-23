package common.dto;

import java.io.Serializable;

//Можно добавить поддержку разных типов сообщений... в консоли ага. PNG->ASCII графика, поехали
public record MessageContent(String content) implements Serializable {

}

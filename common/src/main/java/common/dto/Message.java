package common.dto;

import java.io.Serializable;
import java.time.LocalDateTime;

public class Message implements Serializable {

	private final long messageId;
	private final MessageContent content;
	private final String from;
	private final String to;
	private final LocalDateTime dispatchTime;
	private LocalDateTime seenTime;
	public Message(long messageId, MessageContent content,
	               String from, String to,
	               LocalDateTime dispatchTime, LocalDateTime seenTime){
		this.messageId = messageId;
		this.content = content;
		this.from = from;
		this.to = to;
		this.dispatchTime = dispatchTime;
		this.seenTime = seenTime;
	}

	public Message(long messageId, MessageContent content, String from, String to, LocalDateTime dispatchTime){
		this(messageId, content, from, to, dispatchTime, null);
	}

	public Message(long messageId, MessageContent content, String from, String to){
		this(messageId, content, from, to, LocalDateTime.now(), null);
	}

	public MessageContent content() {
		return content;
	}

	public long messageId(){
		return messageId;
	}

	public String from() {
		return from;
	}

	public String to() {
		return to;
	}

	public LocalDateTime dispatchTime() {
		return dispatchTime;
	}

	public LocalDateTime seenTime() {
		return seenTime;
	}

	public void setSeenTime(LocalDateTime seenTime) {
		this.seenTime = seenTime;
	}

	public String toString(){
		return "id: " + messageId +
				" from: " + from +
				" to: " + (to == null ? "(broadcast)" : to) +
				" dispatch: " + dispatchTime +
				" seen: " + seenTime +
				" content: " + content;
	}
}

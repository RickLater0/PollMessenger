package common.dto;

import java.io.Serializable;
import java.time.LocalDateTime;

public class Message implements Serializable {

	private final long messageId;
	private final MessageContent content;
	private final User from;
	private final User to;
	private final LocalDateTime dispatchTime;
	private LocalDateTime seenTime;
	public Message(long messageId, MessageContent content, User from, User to, LocalDateTime dispatchTime, LocalDateTime seenTime){
		this.messageId = messageId;
		this.content = content;
		this.from = from;
		this.to = to;
		this.dispatchTime = dispatchTime;
		this.seenTime = seenTime;
	}

	public Message(long messageId, MessageContent content, User from, User to, LocalDateTime dispatchTime){
		this(messageId, content, from, to, dispatchTime, null);
	}

	public Message(long messageId, MessageContent content, User from, User to){
		this(messageId, content, from, to, LocalDateTime.now(), null);
	}

	public MessageContent content() {
		return content;
	}

	public long messageId(){
		return messageId;
	}

	public User from() {
		return from;
	}

	public User to() {
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
}

/**
 * @author wlapka
 *
 * @created May 7, 2014 4:15:07 PM
 */
package net.thoiry.lapka.correlationidentifier;

/**
 * @author wlapka
 * 
 */
public class Message {

	private final Long messageId;
	private final Long correlationId;
	private final String body;

	public Message(Long messageId, Long correlationId, String body) {
		this.messageId = messageId;
		this.correlationId = correlationId;
		this.body = body;
	}

	public Long getMessageId() {
		return messageId;
	}

	public Long getCorrelationId() {
		return correlationId;
	}

	public String getBody() {
		return body;
	}

	@Override
	public String toString() {
		return "Message [messageId=" + messageId + ", correlationId=" + correlationId + ", body=" + body + "]";
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((correlationId == null) ? 0 : correlationId.hashCode());
		result = prime * result + ((messageId == null) ? 0 : messageId.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		Message other = (Message) obj;
		if (correlationId == null) {
			if (other.correlationId != null)
				return false;
		} else if (!correlationId.equals(other.correlationId))
			return false;
		if (messageId == null) {
			if (other.messageId != null)
				return false;
		} else if (!messageId.equals(other.messageId))
			return false;
		return true;
	}

}

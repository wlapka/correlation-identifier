/**
 * @author wlapka
 *
 * @created May 9, 2014 11:04:52 AM
 */
package net.thoiry.lapka.correlationidentifier;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * @author wlapka
 * 
 */
public class ReplyChannelImpl<E, T> implements ReplyChannel<E, T> {

	private final ConcurrentMap<E, T> replyMap = new ConcurrentHashMap<>();

	@Override
	public T remove(E messageId) {
		return this.replyMap.remove(messageId);
	}

	@Override
	public T put(E messageId, T message) {
		return this.replyMap.put(messageId, message);
	}

}

/**
 * @author wlapka
 *
 * @created May 9, 2014 11:11:25 AM
 */
package net.thoiry.lapka.correlationidentifier;

/**
 * @author wlapka
 * 
 * @param <E>
 * @param <T>
 */
public interface ReplyChannel<E, T> {

	T remove(E messageId);

	T put(E messageId, T message);

}
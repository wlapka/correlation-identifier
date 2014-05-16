/**
 * @author wlapka
 *
 * @created May 16, 2014 11:29:06 AM
 */
package net.thoiry.lapka.correlationidentifier;

/**
 * @author wlapka
 * 
 */
public interface Pipe<O> {

	boolean send(O message);

	O receive();

}

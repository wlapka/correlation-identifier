/**
 * @author wlapka
 *
 * @created May 16, 2014 11:29:41 AM
 */
package net.thoiry.lapka.correlationidentifier;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * @author wlapka
 * 
 */
public class PipeImpl<O> implements Pipe<O> {

	private final BlockingQueue<O> queue = new LinkedBlockingQueue<>();

	@Override
	public boolean send(O message) {
		return this.queue.offer(message);
	}

	@Override
	public O receive() {
		return this.queue.poll();
	}

}

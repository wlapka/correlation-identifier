/**
 * @author wlapka
 *
 * @created May 7, 2014 4:18:59 PM
 */
package net.thoiry.lapka.correlationidentifier;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author wlapka
 * 
 */
public class Requestor implements Runnable {

	private static final Logger LOGGER = LoggerFactory.getLogger(Requestor.class);
	private static final int TIMEOUTINSECONDS = 1;
	private static final AtomicLong NEXTID = new AtomicLong(1);
	private final BlockingQueue<Message> requestQueue;
	private final ConcurrentMap<Long, Message> replyMap;
	private final Map<Long, Message> sentRequests = new HashMap<>();
	private volatile boolean stop = false;

	public Requestor(BlockingQueue<Message> outQueue, ConcurrentMap<Long, Message> replyMap) {
		this.requestQueue = outQueue;
		this.replyMap = replyMap;
	}

	@Override
	public void run() {
		while (!this.stop) {
			try {
				this.processReplies();
				Long messageId = NEXTID.getAndIncrement();
				Message message = new Message(messageId, null, "Message number " + messageId + " from thread "
						+ Thread.currentThread().getId());
				if (!this.requestQueue.offer(message, TIMEOUTINSECONDS, TimeUnit.SECONDS)) {
					LOGGER.info("Timeout occured while trying to send request since queue full.");
					continue;
				}
				this.sentRequests.put(messageId, message);
				LOGGER.info("Request sent: '{}'.", message);
			} catch (InterruptedException e) {
				LOGGER.info("Interrupted exception occured", e);
				throw new RuntimeException(e.getMessage(), e);
			}
		}
	}

	private void processReplies() {
		for (Iterator<Long> iterator = this.sentRequests.keySet().iterator(); iterator.hasNext();) {
			Long correlationId = iterator.next();
			Message reply = this.replyMap.remove(correlationId);
			if (reply != null) {
				Message request = this.sentRequests.get(reply.getCorrelationId());
				LOGGER.info("{}: Received reply for request: {}.", Thread.currentThread().getId(), request);
				iterator.remove();
			}
		}
	}

	public void stop() {
		this.stop = true;
		LOGGER.info("Received stop signal");
	}
}

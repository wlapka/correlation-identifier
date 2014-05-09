/**
 * @author wlapka
 *
 * @created May 7, 2014 5:42:03 PM
 */
package net.thoiry.lapka.correlationidentifier;

import java.util.Random;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.DelayQueue;
import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author wlapka
 * 
 */
public class Replier implements Runnable {

	private static final Logger LOGGER = LoggerFactory.getLogger(Replier.class);
	private static final int TIMEOUTINSECONDS = 1;
	private static final AtomicLong NEXTID = new AtomicLong();
	private static final int MAXDELAY = 1;
	private final BlockingQueue<Message> requestQueue;
	private final ConcurrentMap<Long, Message> replyMap;
	private final DelayQueue<DelayedMessage> delayedQueue = new DelayQueue<>();
	private final Random random = new Random();
	private volatile boolean stop = false;

	public Replier(BlockingQueue<Message> requestQueue, ConcurrentMap<Long, Message> replyMap) {
		this.requestQueue = requestQueue;
		this.replyMap = replyMap;
	}

	@Override
	public void run() {
		while (!this.stop) {
			try {
				Message request = this.requestQueue.poll(TIMEOUTINSECONDS, TimeUnit.SECONDS);
				if (request != null) {
					LOGGER.info("Received request {}", request);
					this.delayMessage(request);
				}
				this.sendReplies();
			} catch (InterruptedException e) {
				LOGGER.error("Interrupted exception occured.", e);
				throw new RuntimeException(e.getMessage(), e);
			}
		}
	}

	private void delayMessage(Message message) {
		long delay = random.nextInt(MAXDELAY);
		DelayedMessage delayedMessage = new DelayedMessage(message, delay);
		this.delayedQueue.add(delayedMessage);
		LOGGER.info("Message {} added to delay queue.", message);
	}

	private void sendReplies() throws InterruptedException {
		DelayedMessage delayedRequest = this.delayedQueue.poll();
		while (delayedRequest != null) {
			Message request = delayedRequest.getMessage();
			this.sendReply(request);
			delayedRequest = this.delayedQueue.poll();
		}
	}

	private void sendReply(Message request) throws InterruptedException {
		Long replyId = NEXTID.getAndIncrement();
		Message reply = new Message(replyId, request.getMessageId(), "Reply for " + request.getBody());
		this.replyMap.put(reply.getCorrelationId(), reply);
		LOGGER.info("Sent reply {}", reply);
	}

	public void stop() {
		this.stop = true;
		LOGGER.info("Received stop signal");
	}

	private static class DelayedMessage implements Delayed {

		private final Message message;
		private final long delay;
		private final long origin;

		public DelayedMessage(Message message, long delay) {
			this.message = message;
			this.delay = delay;
			this.origin = System.currentTimeMillis();
		}

		public Message getMessage() {
			return message;
		}

		@Override
		public int compareTo(Delayed delayed) {
			if (delayed == this) {
				return 0;
			}
			long d = (this.getDelay(TimeUnit.MILLISECONDS) - delayed.getDelay(TimeUnit.MILLISECONDS));
			return ((d == 0) ? 0 : ((d < 0) ? -1 : 1));
		}

		@Override
		public long getDelay(TimeUnit unit) {
			return unit.convert(delay - (System.currentTimeMillis() - origin), TimeUnit.MILLISECONDS);
		}
	}
}

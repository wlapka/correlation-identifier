/**
 * @author wlapka
 *
 * @created May 7, 2014 5:42:03 PM
 */
package net.thoiry.lapka.correlationidentifier;

import java.util.List;
import java.util.Random;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
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
	private static final int MAXDELAY = 5;
	private final DelayQueue<DelayedMessage> delayedQueue = new DelayQueue<>();
	private final Random random = new Random();
	private volatile boolean stop = false;
	private final BlockingQueue<Message> requestQueue;
	private final ReplyChannel<Long, Message> replyChannel;
	private final CountDownLatch countDownLatch;
	private final List<Requestor> observers = new CopyOnWriteArrayList<>();

	public Replier(BlockingQueue<Message> requestQueue, ReplyChannel<Long, Message> replyChannel,
			CountDownLatch countDownLatch) {
		this.requestQueue = requestQueue;
		this.replyChannel = replyChannel;
		this.countDownLatch = countDownLatch;
	}

	public void addObserver(Requestor observer) {
		this.observers.add(observer);
	}

	public void removeObserver(Requestor observer) {
		this.observers.remove(observer);
	}

	@Override
	public void run() {
		try {
			while (!this.stop) {
				Message request = this.requestQueue.poll(TIMEOUTINSECONDS, TimeUnit.SECONDS);
				if (request != null) {
					LOGGER.info("Received request {}", request);
					this.delayMessage(request);
				}
				this.sendReplies();
			}
			this.sendPendingReplies();
		} catch (InterruptedException e) {
			LOGGER.error("Interrupted exception occured.", e);
			Thread.currentThread().interrupt();
			throw new RuntimeException(e.getMessage(), e);
		} finally {
			if (this.countDownLatch != null) {
				this.countDownLatch.countDown();
			}
		}
	}

	private void delayMessage(Message message) {
		long delay = random.nextInt(MAXDELAY);
		DelayedMessage delayedMessage = new DelayedMessage(message, delay);
		this.delayedQueue.add(delayedMessage);
		LOGGER.info("Message {} added to delay queue.", message);
	}

	private void sendPendingReplies() throws InterruptedException {
		LOGGER.info("Sending pending replies. Pending queue size: {}", this.delayedQueue.size());
		while (this.delayedQueue.size() > 0) {
			this.sendReplies();
		}
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
		this.replyChannel.put(reply.getCorrelationId(), reply);
		LOGGER.info("Sent reply {}", reply);
		for (Requestor observer : this.observers) {
			observer.onReply(reply);
		}
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

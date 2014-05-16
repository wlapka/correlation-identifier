/**
 * @author wlapka
 *
 * @created May 7, 2014 5:42:03 PM
 */
package net.thoiry.lapka.correlationidentifier;

import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.DelayQueue;
import java.util.concurrent.Delayed;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
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
	private static final AtomicLong NEXTID = new AtomicLong(1);
	private static final int MAXDELAY = 2;
	private static final int NUMBEROFINTERNALTHREADS = 1;
	private final DelayQueue<DelayedMessage> delayedQueue = new DelayQueue<>();
	private final Random random = new Random();
	private final ConcurrentMap<Long, Requestor> observers = new ConcurrentHashMap<>();
	private final CountDownLatch internalCountDownLatch = new CountDownLatch(NUMBEROFINTERNALTHREADS);
	private final ExecutorService executorService = Executors.newFixedThreadPool(NUMBEROFINTERNALTHREADS);
	private final ReplySender replySender = new ReplySender(internalCountDownLatch, delayedQueue, observers);
	private volatile boolean stop = false;
	private final Pipe<Message> requestQueue;
	private final CountDownLatch countDownLatch;

	public Replier(Pipe<Message> requestQueue, CountDownLatch countDownLatch) {
		this.requestQueue = requestQueue;
		this.countDownLatch = countDownLatch;
		this.executorService.submit(replySender);
	}

	public void addObserver(Long correlationId, Requestor observer) {
		this.observers.put(correlationId, observer);
	}

	public void removeObserver(Long correlationId) {
		this.observers.remove(correlationId);
	}

	@Override
	public void run() {
		try {
			while (!this.stop) {
				Message request = this.requestQueue.receive();
				if (request != null) {
					LOGGER.info("Received request {}", request);
					this.delayMessage(request);
				}
			}
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

	public void stop() {
		try {
			this.stop = true;
			this.replySender.stop();
			this.internalCountDownLatch.await();
			this.executorService.shutdown();
			LOGGER.info("Received stop signal");
		} catch (InterruptedException e) {
			LOGGER.error("InterruptedException occured", e);
			Thread.currentThread().interrupt();
			throw new RuntimeException(e.getMessage(), e);
		}
	}

	private static class ReplySender implements Runnable {

		private static final Logger LOGGER = LoggerFactory.getLogger(ReplySender.class);
		private volatile boolean stop = false;
		private final CountDownLatch countDownLatch;
		private final DelayQueue<DelayedMessage> delayedQueue;
		private final ConcurrentMap<Long, Requestor> observers;

		public ReplySender(CountDownLatch countDownLatch, DelayQueue<DelayedMessage> delayedQueue,
				ConcurrentMap<Long, Requestor> observers) {
			this.countDownLatch = countDownLatch;
			this.delayedQueue = delayedQueue;
			this.observers = observers;
		}

		@Override
		public void run() {
			try {
				while (!this.stop) {
					this.sendReplies();
				}
				this.sendPendingReplies();
			} catch (InterruptedException e) {
				LOGGER.error("Received interrupted exception.", e);
				Thread.currentThread().interrupt();
				throw new RuntimeException(e.getMessage(), e);
			} finally {
				if (this.countDownLatch != null) {
					this.countDownLatch.countDown();
				}
			}
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
			Requestor observer = this.observers.get(reply.getCorrelationId());
			if (observer != null) {
				observer.receiveReply(reply);
			}
			LOGGER.info("Sent reply {}", reply);
		}

		public void stop() {
			this.stop = true;
			LOGGER.info("Received stop signal");
		}

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

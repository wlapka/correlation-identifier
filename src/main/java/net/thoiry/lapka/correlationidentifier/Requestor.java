/**
 * @author wlapka
 *
 * @created May 7, 2014 4:18:59 PM
 */
package net.thoiry.lapka.correlationidentifier;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
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
	// private static final int QUERYFREQUENCYINMILISECONDS = 500;
	private static final AtomicLong NEXTID = new AtomicLong(1);
	private final BlockingQueue<Message> requestQueue;
	private final Replier replier;
	private final CountDownLatch countDownLatch;
	private volatile boolean stop = false;
	private Message reply = null;

	public Requestor(BlockingQueue<Message> outQueue, Replier replier, CountDownLatch countDownLatch) {
		this.requestQueue = outQueue;
		this.replier = replier;
		this.countDownLatch = countDownLatch;
	}

	@Override
	public void run() {
		try {
			while (!this.stop) {
				Long messageId = NEXTID.getAndIncrement();
				Message message = new Message(messageId, null, "Message number " + messageId + " from thread "
						+ Thread.currentThread().getId());
				this.replier.addObserver(messageId, this);
				if (!this.requestQueue.offer(message, TIMEOUTINSECONDS, TimeUnit.SECONDS)) {
					LOGGER.info("Timeout occured while trying to send request since queue full.");
					this.replier.removeObserver(messageId);
					continue;
				}
				LOGGER.info("Sent request: '{}'.", message);
				this.getReply(message);

			}
		} catch (InterruptedException e) {
			LOGGER.info("Interrupted exception occured", e);
			Thread.currentThread().interrupt();
			throw new RuntimeException(e.getMessage(), e);
		} finally {
			if (this.countDownLatch != null) {
				this.countDownLatch.countDown();
			}
		}
	}

	private void getReply(Message request) throws InterruptedException {
		synchronized (this) {
			while (this.reply == null) {
				LOGGER.info("Waiting for reply for request: {}", request);
				this.wait();
			}
			if (this.reply != null) {
				LOGGER.info("Received reply for request: {}.", request);
				this.reply = null;
				return;
			}
		}
	}

	public void setReply(Message reply) {
		synchronized (this) {
			LOGGER.info("Received reply: '{}'", reply);
			this.reply = reply;
			this.replier.removeObserver(reply.getCorrelationId());
			this.notify();
		}
	}

	// private void getReply(Message request) throws InterruptedException {
	// while (true) {
	// Message reply = this.replyChannel.remove(request.getMessageId());
	// if (reply != null) {
	// LOGGER.info("{}: Received reply for request: {}.",
	// Thread.currentThread().getId(), request);
	// return;
	// }
	// Thread.sleep(QUERYFREQUENCYINMILISECONDS);
	// }
	// }

	public void stop() {
		this.stop = true;
		LOGGER.info("Received stop signal");
	}
}

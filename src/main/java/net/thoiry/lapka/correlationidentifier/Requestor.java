/**
 * @author wlapka
 *
 * @created May 7, 2014 4:18:59 PM
 */
package net.thoiry.lapka.correlationidentifier;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author wlapka
 * 
 */
public class Requestor implements Runnable {

	private static final Logger LOGGER = LoggerFactory.getLogger(Requestor.class);
	private static final AtomicLong NEXTID = new AtomicLong(1);
	private final Pipe<Message> outPipe;
	private final Replier replier;
	private final CountDownLatch countDownLatch;
	private volatile boolean stop = false;
	private Message reply = null;

	public Requestor(Pipe<Message> outPipe, Replier replier, CountDownLatch countDownLatch) {
		this.outPipe = outPipe;
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
				if (!this.outPipe.send(message)) {
					LOGGER.info("Message {} couldn't be sent.", message);
					this.replier.removeObserver(messageId);
					continue;
				}
				LOGGER.info("Sent request: '{}'.", message);
				this.waitForReply(message);
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

	private void waitForReply(Message request) throws InterruptedException {
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

	public void receiveReply(Message reply) {
		synchronized (this) {
			LOGGER.info("Received reply: '{}'", reply);
			this.reply = reply;
			this.replier.removeObserver(reply.getCorrelationId());
			this.notify();
		}
	}

	public void stop() {
		this.stop = true;
		LOGGER.info("Received stop signal");
	}
}

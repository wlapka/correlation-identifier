/**
 * @author wlapka
 *
 * @created May 7, 2014 4:18:59 PM
 */
package net.thoiry.lapka.correlationidentifier;

import java.util.concurrent.BlockingQueue;
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
	private final ReplyChannel<Long, Message> replyChannel;
	private volatile boolean stop = false;

	public Requestor(BlockingQueue<Message> outQueue, ReplyChannel<Long, Message> replyChannel) {
		this.requestQueue = outQueue;
		this.replyChannel = replyChannel;
	}

	@Override
	public void run() {
		while (!this.stop) {
			try {
				Long messageId = NEXTID.getAndIncrement();
				Message message = new Message(messageId, null, "Message number " + messageId + " from thread "
						+ Thread.currentThread().getId());
				if (!this.requestQueue.offer(message, TIMEOUTINSECONDS, TimeUnit.SECONDS)) {
					LOGGER.info("Timeout occured while trying to send request since queue full.");
					continue;
				}
				LOGGER.info("Sent request: '{}'.", message);
				this.getReply(message);
			} catch (InterruptedException e) {
				LOGGER.info("Interrupted exception occured", e);
				throw new RuntimeException(e.getMessage(), e);
			}
		}
	}

	private void getReply(Message request) throws InterruptedException {
		Message reply = null;
		synchronized (this) {
			while (reply == null) {
				LOGGER.info("Waiting for reply {}, messageId: {}", this, request.getMessageId());
				this.wait();
				LOGGER.info("Woken up {} {}", this, request.getMessageId());
				reply = this.replyChannel.remove(request.getMessageId());
				if (reply != null) {
					LOGGER.info("{}: Received reply for request: {}.", Thread.currentThread().getId(), request);
					return;
				}
			}
		}
	}

	public void onReply(Message reply) {
		synchronized (this) {
			LOGGER.info("Received notification about reply '{}', {}", reply);
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

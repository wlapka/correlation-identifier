/**
 * @author wlapka
 *
 * @created May 7, 2014 5:25:01 PM
 */
package net.thoiry.lapka.correlationidentifier;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author wlapka
 * 
 */
public class CorrelationService {

	private static final Logger LOGGER = LoggerFactory.getLogger(CorrelationService.class);
	private static final int NUMBEROFTHREADS = 2;
	private final CountDownLatch countDownLatch = new CountDownLatch(NUMBEROFTHREADS);
	private final Pipe<Message> requestPipe = new PipeImpl<>();
	private final Replier replier = new Replier(requestPipe, countDownLatch);
	private final Requestor requestor1 = new Requestor(requestPipe, replier, countDownLatch);
	private final Requestor requestor2 = new Requestor(requestPipe, replier, countDownLatch);
	private final Requestor requestor3 = new Requestor(requestPipe, replier, countDownLatch);
	private final Requestor requestor4 = new Requestor(requestPipe, replier, countDownLatch);
	private final ExecutorService executorService = Executors.newFixedThreadPool(NUMBEROFTHREADS);

	private void start() throws InterruptedException {
		this.executorService.submit(requestor1);
		this.executorService.submit(requestor2);
		this.executorService.submit(requestor3);
		this.executorService.submit(requestor4);
		this.executorService.submit(replier);
	}

	private void stop() throws InterruptedException {
		this.requestor1.stop();
		this.requestor2.stop();
		this.requestor3.stop();
		this.requestor4.stop();
		this.replier.stop();
		this.countDownLatch.await();
		this.executorService.shutdown();
	}

	public static void main(String[] args) throws InterruptedException {
		LOGGER.info("Application started");
		CorrelationService correlationService = new CorrelationService();
		correlationService.start();
		LOGGER.info("Correlation service started.");
		Thread.sleep(5000);
		correlationService.stop();
		LOGGER.info("Correlation service finished.");
		LOGGER.info("Application finished");
	}
}

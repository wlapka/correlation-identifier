/**
 * @author wlapka
 *
 * @created May 7, 2014 5:25:01 PM
 */
package net.thoiry.lapka.correlationidentifier;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author wlapka
 * 
 */
public class CorrelationService {

	private static final Logger LOGGER = LoggerFactory.getLogger(CorrelationService.class);
	private static final int QUEUECAPACITY = 100;
	private static final int THREADPOOLSIZE = 10;
	private final BlockingQueue<Message> requestQueue = new ArrayBlockingQueue<>(QUEUECAPACITY);
	private final ConcurrentMap<Long, Message> replyMap = new ConcurrentHashMap<>();
	private final Requestor requestor = new Requestor(requestQueue, replyMap);
	private final Requestor requestor1 = new Requestor(requestQueue, replyMap);
	private final Requestor requestor2 = new Requestor(requestQueue, replyMap);
	private final Requestor requestor3 = new Requestor(requestQueue, replyMap);
	private final Requestor requestor4 = new Requestor(requestQueue, replyMap);
	private final Replier replier = new Replier(requestQueue, replyMap);
	private final List<Future> futures = new ArrayList<>();
	private final ExecutorService executorService = Executors.newFixedThreadPool(THREADPOOLSIZE);

	private void start() throws InterruptedException {
		this.futures.add(this.executorService.submit(requestor));
		this.futures.add(this.executorService.submit(requestor1));
		this.futures.add(this.executorService.submit(requestor2));
		this.futures.add(this.executorService.submit(requestor3));
		this.futures.add(this.executorService.submit(requestor4));
		this.futures.add(this.executorService.submit(replier));
		LOGGER.info("Correlation service started.");
	}

	private void stop() {
		this.requestor.stop();
		this.requestor1.stop();
		this.requestor2.stop();
		this.requestor3.stop();
		this.requestor4.stop();
		this.replier.stop();
		try {
			for (Future future : this.futures) {
				future.get();
			}
		} catch (ExecutionException e) {
			LOGGER.error("Execution exception occured.", e);
			throw new RuntimeException(e.getMessage(), e);
		} catch (InterruptedException e) {
			LOGGER.error("Interrupted exception occured.", e);
			throw new RuntimeException(e.getMessage(), e);
		}
		this.executorService.shutdown();
	}

	public static void main(String[] args) throws InterruptedException {
		LOGGER.info("Application started");
		CorrelationService correlationService = new CorrelationService();
		correlationService.start();
		Thread.sleep(10000);
		correlationService.stop();
		LOGGER.info("Application finished");
	}
}

/**
 * @author wlapka
 *
 * @created Mar 18, 2014 9:46:50 AM
 */
package net.thoiry.lapka.eip.resequencer;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author wlapka
 * 
 */
public class SequenceSender implements Runnable {
	private static final Logger LOGGER = LoggerFactory.getLogger(SequenceSender.class);
	private static final int TIMEOUTINSECONDS = 5;
	private static final AtomicInteger nextSerialNumber = new AtomicInteger(1);
	private final BlockingQueue<Message> outQueue;
	private volatile boolean stop = false;

	public SequenceSender(BlockingQueue<Message> outQueue) {
		this.outQueue = outQueue;
	}

	@Override
	public void run() {
		while (!stop) {
			try {
				int sequenceNumber = nextSerialNumber.getAndIncrement();
				Message message = new Message(sequenceNumber, "Message number: " + sequenceNumber);
				if (!outQueue.offer(message, TIMEOUTINSECONDS, TimeUnit.SECONDS)) {
					LOGGER.info("Timeout occured since queue full. Stopping sender");
					return;
				}
				LOGGER.info("Sent message: {}.", message);
			} catch (InterruptedException e) {
				LOGGER.info("Interrupted exception occured", e);
				throw new RuntimeException(e.getMessage(), e);
			}
		}
	}

	public void stop() {
		this.stop = true;
		LOGGER.info("Received stop signal.");
	}
}

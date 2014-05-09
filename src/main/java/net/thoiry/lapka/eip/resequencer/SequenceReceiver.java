/**
 * @author wlapka
 *
 * @created Mar 18, 2014 2:26:39 PM
 */
package net.thoiry.lapka.eip.resequencer;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author wlapka
 * 
 */
public class SequenceReceiver implements Runnable {
	private static final Logger LOGGER = LoggerFactory.getLogger(SequenceReceiver.class);
	private final BlockingQueue<Message> inQueue;
	private final CountDownLatch countDownLatch;
	private boolean stop = false;

	public SequenceReceiver(BlockingQueue<Message> inQueue, CountDownLatch countDownLatch) {
		this.inQueue = inQueue;
		this.countDownLatch = countDownLatch;
	}

	@Override
	public void run() {
		try {
			while (!this.stop) {
				Message message = inQueue.poll();
				if (message != null) {
					LOGGER.info("Received message: {}.", message);
				}
			}
		} finally {
			if (this.countDownLatch != null) {
				this.countDownLatch.countDown();
			}
		}
	}

	public void stop() {
		this.stop = true;
		LOGGER.info("Received stop signal.");
	}
}

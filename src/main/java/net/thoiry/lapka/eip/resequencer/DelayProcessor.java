/**
 * @author wlapka
 *
 * @created Mar 18, 2014 11:08:02 AM
 */
package net.thoiry.lapka.eip.resequencer;

import java.util.Random;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.DelayQueue;
import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author wlapka
 * 
 */
public class DelayProcessor implements Runnable {
	private static final Logger LOGGER = LoggerFactory.getLogger(DelayProcessor.class);
	private static final int TIMEOUTINSECONDS = 5;
	private static final int MAXDELAY = 2;
	private final BlockingQueue<Message> inQueue;
	private final BlockingQueue<Message> outQueue;
	private final DelayQueue<DelayedMessage> delayedQueue = new DelayQueue<>();
	private boolean stop = false;

	public DelayProcessor(BlockingQueue<Message> inQueue, BlockingQueue<Message> outQueue) {
		this.inQueue = inQueue;
		this.outQueue = outQueue;
	}

	@Override
	public void run() {
		while (!stop) {
			Message message = inQueue.poll();
			if (message != null) {
				LOGGER.info("Received message: " + message);
				this.delayMessage(message);
			}
			this.addDelayedMessagesToOutQueue();
		}
	}

	private void delayMessage(Message message) {
		Random random = new Random();
		long delay = random.nextInt(MAXDELAY);
		DelayedMessage delayedMessage = new DelayedMessage(message, delay);
		this.delayedQueue.add(delayedMessage);
	}

	private void addDelayedMessagesToOutQueue() {
		DelayedMessage delayedMessage = this.delayedQueue.poll();
		while (delayedMessage != null) {
			Message message = delayedMessage.getMessage();
			try {
				if (!this.outQueue.offer(message, TIMEOUTINSECONDS, TimeUnit.SECONDS)) {
					LOGGER.info("Timeout occured since outputQueue full. Quitting.");
					this.stop();
					return;
				}
			} catch (InterruptedException e) {
				throw new RuntimeException(e.getMessage(), e);
			}
			LOGGER.info("Sent message: {}.", message);
			delayedMessage = this.delayedQueue.poll();
		}
	}

	public void stop() {
		this.stop = true;
		LOGGER.info("Received stop signal.");
	}

	private static class DelayedMessage implements Delayed {
		private final Message message;
		private final long origin;
		private final long delay;

		public DelayedMessage(Message message, long delay) {
			this.message = message;
			this.origin = System.currentTimeMillis();
			this.delay = delay;
		}

		public Message getMessage() {
			return this.message;
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
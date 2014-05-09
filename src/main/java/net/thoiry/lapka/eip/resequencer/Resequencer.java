/**
 * @author wlapka
 *
 * @created Mar 18, 2014 11:58:59 AM
 */
package net.thoiry.lapka.eip.resequencer;

import java.util.Comparator;
import java.util.Iterator;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author wlapka
 * 
 */
public class Resequencer implements Runnable {
	private static final Logger LOGGER = LoggerFactory.getLogger(Resequencer.class);
	private static final int TIMEOUTINSECONDS = 5;
	private static final Comparator<Message> ASCMSGCOMPARATOR = new Comparator<Message>() {
		@Override
		public int compare(Message m1, Message m2) {
			return m1.getSequenceNumber().compareTo(m2.getSequenceNumber());
		}
	};
	private final Set<Message> pendingMessages = new TreeSet<>(ASCMSGCOMPARATOR);
	private final BlockingQueue<Message> inQueue;
	private final BlockingQueue<Message> sequenceQueue;
	private final CountDownLatch countDownLatch;
	private int currSequenceNumber = 1;
	private boolean stop = false;

	public Resequencer(BlockingQueue<Message> inQueue, BlockingQueue<Message> sequenceQueue,
			CountDownLatch countDownLatch) {
		this.inQueue = inQueue;
		this.sequenceQueue = sequenceQueue;
		this.countDownLatch = countDownLatch;
	}

	@Override
	public void run() {
		try {
			while (!this.stop) {
				Message message = this.inQueue.poll();
				if (message != null) {
					LOGGER.info("Received message: {}.", message);
					this.pendingMessages.add(message);
					if (message.getSequenceNumber() == currSequenceNumber)
						this.sendSequencedMessages();
				}
			}
		} finally {
			if (this.countDownLatch != null) {
				this.countDownLatch.countDown();
			}
		}
	}

	private void sendSequencedMessages() {
		try {
			for (Iterator<Message> iterator = this.pendingMessages.iterator(); iterator.hasNext();) {
				Message message = iterator.next();
				if (message.getSequenceNumber() != currSequenceNumber) {
					LOGGER.info("Stopping sending, since next message is unordered: {}.", message);
					return;
				}
				if (!this.sequenceQueue.offer(message, TIMEOUTINSECONDS, TimeUnit.SECONDS)) {
					LOGGER.info("Timeout occured since queue full. Quitting.");
					this.stop();
					return;
				}
				iterator.remove();
				currSequenceNumber += 1;
				LOGGER.info("Sent message: {}.", message);
			}
		} catch (InterruptedException e) {
			LOGGER.info("Got interrupted exception", e);
			throw new RuntimeException(e.getMessage(), e);
		}
	}

	public void stop() {
		this.stop = true;
		LOGGER.info("Received stop signal.");
	}
}
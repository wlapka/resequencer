/**
 * @author wlapka
 *
 * @created Mar 18, 2014 10:39:20 AM
 */
package net.thoiry.lapka.eip.resequencer;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author wlapka
 * 
 */
public class ResequencerServiceCmd {

	private static final Logger LOGGER = LoggerFactory.getLogger(ResequencerServiceCmd.class);
	private static final int QUEUECAPACITY = 100;
	private static final int THREADPOOLSIZE = 10;
	private static final int EXECUTIONTIME = 2000;
	private static final int NUMBEROFTHREADS = 10;
	private final CountDownLatch countDownLatch = new CountDownLatch(NUMBEROFTHREADS);
	private final BlockingQueue<Message> inQueue = new ArrayBlockingQueue<>(QUEUECAPACITY);
	private final BlockingQueue<Message> outQueue = new ArrayBlockingQueue<>(QUEUECAPACITY);
	private final BlockingQueue<Message> sequenceQueue = new ArrayBlockingQueue<>(QUEUECAPACITY);
	private final SequenceSender sender1 = new SequenceSender(inQueue, countDownLatch);
	private final SequenceSender sender2 = new SequenceSender(inQueue, countDownLatch);
	private final SequenceSender sender3 = new SequenceSender(inQueue, countDownLatch);
	private final SequenceSender sender4 = new SequenceSender(inQueue, countDownLatch);
	private final DelayProcessor delayProcessor1 = new DelayProcessor(inQueue, outQueue, countDownLatch);
	private final DelayProcessor delayProcessor2 = new DelayProcessor(inQueue, outQueue, countDownLatch);
	private final DelayProcessor delayProcessor3 = new DelayProcessor(inQueue, outQueue, countDownLatch);
	private final DelayProcessor delayProcessor4 = new DelayProcessor(inQueue, outQueue, countDownLatch);
	private final Resequencer resequencer = new Resequencer(outQueue, sequenceQueue, countDownLatch);
	private final SequenceReceiver sequenceReceiver = new SequenceReceiver(sequenceQueue, countDownLatch);
	private final ExecutorService executorService = Executors.newFixedThreadPool(THREADPOOLSIZE);

	public void start() {
		executorService.submit(sender1);
		executorService.submit(sender2);
		executorService.submit(sender3);
		executorService.submit(sender4);
		executorService.submit(delayProcessor1);
		executorService.submit(delayProcessor2);
		executorService.submit(delayProcessor3);
		executorService.submit(delayProcessor4);
		executorService.submit(resequencer);
		executorService.submit(sequenceReceiver);
	}

	public void stop() throws InterruptedException, ExecutionException {
		this.sender1.stop();
		this.sender2.stop();
		this.sender3.stop();
		this.sender4.stop();
		this.delayProcessor1.stop();
		this.delayProcessor2.stop();
		this.delayProcessor3.stop();
		this.delayProcessor4.stop();
		this.resequencer.stop();
		this.sequenceReceiver.stop();
		this.countDownLatch.await();
		this.executorService.shutdown();
		
	}

	public static void main(String[] args) {
		LOGGER.info("Begin of execution.");
		ResequencerServiceCmd resequencerService = new ResequencerServiceCmd();
		try {
			resequencerService.start();
			Thread.sleep(EXECUTIONTIME);
			resequencerService.stop();
		} catch (InterruptedException e) {
			LOGGER.error("Interrupted exception occured", e);
			throw new RuntimeException(e.getMessage(), e);
		} catch (ExecutionException e) {
			LOGGER.error("Execution exception occured", e);
			throw new RuntimeException(e.getMessage(), e);
		}
		LOGGER.info("End of execution.");
	}

}

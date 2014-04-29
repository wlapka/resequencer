/**
 * @author wlapka
 *
 * @created Mar 18, 2014 10:39:20 AM
 */
package net.thoiry.lapka.eip.resequencer;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
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
public class ResequencerServiceCmd {

	private static final Logger LOGGER = LoggerFactory.getLogger(ResequencerServiceCmd.class);
	private static final int QUEUECAPACITY = 100;
	private static final int THREADPOOLSIZE = 10;
	private static final int NUMBEROFSENDERS = 4;
	private static final int NUMBEROFDELAYPROCESSORS = 4;
	private static final int EXECUTIONTIME = 200;

	private final BlockingQueue<Message> inQueue = new ArrayBlockingQueue<>(QUEUECAPACITY);
	private final BlockingQueue<Message> outQueue = new ArrayBlockingQueue<>(QUEUECAPACITY);
	private final BlockingQueue<Message> sequenceQueue = new ArrayBlockingQueue<>(QUEUECAPACITY);
	private final SequenceSender sender = new SequenceSender(inQueue);
	private final DelayProcessor delayProcessor = new DelayProcessor(inQueue, outQueue);
	private final Resequencer resequencer = new Resequencer(outQueue, sequenceQueue);
	private final SequenceReceiver sequenceReceiver = new SequenceReceiver(sequenceQueue);
	private final ExecutorService executorService = Executors.newFixedThreadPool(THREADPOOLSIZE);
	private final List<Future> futures = new ArrayList<>();

	public void start() {
		this.futures.addAll(this.submitTasks(executorService, sender, NUMBEROFSENDERS));
		this.futures.addAll(this.submitTasks(executorService, delayProcessor, NUMBEROFDELAYPROCESSORS));
		this.futures.add(executorService.submit(resequencer));
		this.futures.add(executorService.submit(sequenceReceiver));
	}

	private List<Future> submitTasks(ExecutorService executorService, Runnable task, int numberOfSubmissions) {
		List<Future> futures = new ArrayList<>();
		for (int i = 0; i < numberOfSubmissions; i++) {
			futures.add(executorService.submit(task));
		}
		return futures;
	}

	public void stop() throws InterruptedException, ExecutionException {
		this.sender.stop();
		this.delayProcessor.stop();
		this.resequencer.stop();
		this.sequenceReceiver.stop();
		// Wait till all tasks finished
		for (Future future : this.futures) {
			future.get();
		}
		this.executorService.shutdown();
	}

	public static void main(String[] args) {
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
	}

}

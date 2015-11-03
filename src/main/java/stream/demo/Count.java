/**
 * 
 */
package stream.demo;

import java.util.concurrent.atomic.AtomicLong;

import stream.AbstractProcessor;
import stream.Data;

/**
 * @author chris
 *
 */
public class Count extends AbstractProcessor {

	final AtomicLong count = new AtomicLong(0L);

	/**
	 * @see stream.Processor#process(stream.Data)
	 */
	@Override
	public Data process(Data input) {
		count.incrementAndGet();
		return input;
	}

	/**
	 * @see stream.AbstractProcessor#finish()
	 */
	@Override
	public void finish() throws Exception {
		super.finish();
		System.out.println("Finishing after processing " + count.get() + " items.");
	}
}

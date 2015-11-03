/**
 * 
 */
package stream.util;

import stream.Data;
import stream.Processor;

/**
 * @author chris
 * 
 */
public class AlternateAB implements Processor {

	String key = "flag";

	long count = 0L;

	/**
	 * @see stream.Processor#process(stream.Data)
	 */
	@Override
	public Data process(Data input) {

		if (count++ % 2 == 0) {
			input.put(key, "A");
		} else {
			input.put(key, "B");
		}

		return input;
	}

	/**
	 * @return the key
	 */
	public String getKey() {
		return key;
	}

	/**
	 * @param key
	 *            the key to set
	 */
	public void setKey(String key) {
		this.key = key;
	}
}
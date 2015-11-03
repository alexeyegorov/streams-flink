/**
 * 
 */
package stream.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import stream.Data;
import stream.Processor;

/**
 * @author chris
 * 
 */
public class Log implements Processor {

	static Logger log = LoggerFactory.getLogger(Log.class);
	String prefix = "";

	/**
	 * @see stream.Processor#process(stream.Data)
	 */
	@Override
	public Data process(Data input) {
		if (prefix != null) {
			log.info("{} {}", prefix, input);
		} else {
			log.info("{}", input);
		}
		return input;
	}

	/**
	 * @return the prefix
	 */
	public String getPrefix() {
		return prefix;
	}

	/**
	 * @param prefix
	 *            the prefix to set
	 */
	public void setPrefix(String prefix) {
		this.prefix = prefix;
	}
}

/**
 * 
 */
package stream.demo;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import stream.Data;

/**
 * @author chris
 *
 */
public class Enqueue extends stream.flow.Enqueue {

	static Logger log = LoggerFactory.getLogger(Enqueue.class);

	/**
	 * @see stream.flow.Emitter#processMatchingData(stream.Data)
	 */
	@Override
	public Data processMatchingData(Data data) throws Exception {
		log.info("processing matching item {}   (condition: {})", data, this.getCondition());
		return super.processMatchingData(data);
	}

	/**
	 * @see stream.expressions.version2.ConditionedProcessor#matches(stream.Data)
	 */
	@Override
	public boolean matches(Data item) throws Exception {
		boolean match = super.matches(item);
		log.info("item matching? {}", match);
		return match;
	}

	/**
	 * @see stream.flow.Emitter#emit(stream.Data)
	 */
	@Override
	protected void emit(Data data) {
		log.info("writing item {} to queue {}", data, this.sinks[0]);
		super.emit(data);
	}
}

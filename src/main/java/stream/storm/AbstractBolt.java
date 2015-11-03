/*
 *  streams library
 *
 *  Copyright (C) 2011-2014 by Christian Bockermann, Hendrik Blom
 * 
 *  streams is a library, API and runtime environment for processing high
 *  volume data streams. It is composed of three submodules "stream-api",
 *  "stream-core" and "stream-runtime".
 *
 *  The streams library (and its submodules) is free software: you can 
 *  redistribute it and/or modify it under the terms of the 
 *  GNU Affero General Public License as published by the Free Software 
 *  Foundation, either version 3 of the License, or (at your option) any 
 *  later version.
 *
 *  The stream.ai library (and its submodules) is distributed in the hope
 *  that it will be useful, but WITHOUT ANY WARRANTY; without even the implied 
 *  warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU Affero General Public License for more details.
 *
 *  You should have received a copy of the GNU Affero General Public License
 *  along with this program.  If not, see http://www.gnu.org/licenses/.
 */
package stream.storm;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.task.OutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;

/**
 * @author chris
 * 
 */
public abstract class AbstractBolt extends BaseRichBolt {

	/** The unique class ID */
	private static final long serialVersionUID = 5805945428106147592L;
	protected static Logger log = LoggerFactory.getLogger(AbstractBolt.class);

	protected OutputCollector output;
	protected final String xmlConfig;
	protected final String uuid;

	/**
	 * 
	 */
	public AbstractBolt(String xmlConfig, String uuid) {
		this.xmlConfig = xmlConfig;
		this.uuid = uuid;
	}

	/**
	 * @see backtype.storm.topology.IComponent#declareOutputFields(backtype.storm.topology.OutputFieldsDeclarer)
	 */
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		log.debug("Declaring Bolt-output field 'stream.Data' for default output stream");
		declarer.declare(new Fields(TupleWrapper.DATA_KEY));
	}
}
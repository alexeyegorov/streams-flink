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

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import stream.Data;
import stream.data.DataFactory;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

/**
 * @author chris
 * 
 */
public class ClockSpout extends BaseRichSpout {

	/** The unique class ID */
	private static final long serialVersionUID = 812144742121538026L;

	static Logger log = LoggerFactory.getLogger(ClockSpout.class);

	Long interval = 1000L;
	SpoutOutputCollector output;

	public ClockSpout(Long interval) {
		this.interval = interval;
	}

	/**
	 * @see backtype.storm.spout.ISpout#open(java.util.Map,
	 *      backtype.storm.task.TopologyContext,
	 *      backtype.storm.spout.SpoutOutputCollector)
	 */
	@SuppressWarnings("rawtypes")
	@Override
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		this.output = collector;
	}

	/**
	 * @see backtype.storm.spout.ISpout#nextTuple()
	 */
	@Override
	public void nextTuple() {
		try {
			Thread.sleep(interval);
		} catch (Exception e) {
			log.error("Error while taking my interval-nap: {}", e.getMessage());
			if (log.isDebugEnabled())
				e.printStackTrace();
		}

		Data item = DataFactory.create();
		item.put("@time", System.currentTimeMillis());
		output.emit(new Values(item));
	}

	/**
	 * @see backtype.storm.topology.IComponent#declareOutputFields(backtype.storm
	 *      .topology.OutputFieldsDeclarer)
	 */
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		log.debug("Declaring output-field 'stream.Data'");
		declarer.declare(new Fields("stream.Data"));
	}
}

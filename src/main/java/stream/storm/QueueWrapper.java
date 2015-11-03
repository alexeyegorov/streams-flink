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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.task.OutputCollector;
import stream.Data;
import stream.io.Queue;
import stream.io.Sink;

/**
 * @author chris
 * 
 */
public class QueueWrapper implements Queue, Sink, Serializable {

	/** The unique class ID */
	private static final long serialVersionUID = 5528349910849921579L;

	static Logger log = LoggerFactory.getLogger(QueueWrapper.class);

	final OutputCollector collector;
	final String name;

	public QueueWrapper(OutputCollector collector, String name) {
		this.collector = collector;
		this.name = name;
		log.debug("Creating QueueWrapper for queue '{}'", name);
	}

	/**
	 * @see stream.io.Sink#getId()
	 */
	@Override
	public String getId() {
		return name;
	}

	/**
	 * @see stream.io.Sink#write(stream.Data)
	 */
	@Override
	public boolean write(Data item) throws Exception {
		log.debug("Writing to queue '{}'  (item is: {})", name, item);
		log.debug("   using collector {}", collector);
		List<Object> tuple = new ArrayList<Object>();
		tuple.add(item.createCopy());
		log.info("{} -> emitting tuple to '{}'", this, name);
		collector.emit(name, tuple);
		return true;
	}

	@Override
	public void close() throws Exception {

	}

	@Override
	public boolean write(Collection<Data> data) throws Exception {

		for (Data item : data) {
			List<Object> tuple = new ArrayList<Object>();
			tuple.add(item);
			log.info("{} -> emitting tuple to '{}'", this, name);
			collector.emit(name, tuple);
		}

		return true;
	}

	/**
	 * @see stream.io.Sink#setId(java.lang.String)
	 */
	@Override
	public void setId(String id) {
	}

	/**
	 * @see stream.io.Sink#init()
	 */
	@Override
	public void init() throws Exception {

	}

	/**
	 * @see stream.io.Barrel#clear()
	 */
	@Override
	public int clear() {
		return 0;
	}

	/**
	 * @see stream.io.Source#read()
	 */
	@Override
	public Data read() throws Exception {
		return null;
	}

	/**
	 * @see stream.io.Queue#getSize()
	 */
	@Override
	public Integer getSize() {
		return Integer.MAX_VALUE;
	}

	@Override
	public void setCapacity(Integer limit) {
	}

	@Override
	public Integer getCapacity() {
		return Integer.MAX_VALUE;
	}

	/**
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return super.toString() + "[" + this.name + "]";
	}

}

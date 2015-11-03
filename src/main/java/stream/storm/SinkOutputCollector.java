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

import java.util.Collection;

import stream.Data;
import stream.io.Sink;
import backtype.storm.task.OutputCollector;
import backtype.storm.tuple.Values;

/**
 * 
 * @author Thomas Scharrenbach
 * @version 0.9.10
 * @since 0.9.10
 * 
 */
public class SinkOutputCollector implements Sink {

	private OutputCollector outputCollector;
	private String id;

	@Override
	public boolean write(Data item) throws Exception {
		outputCollector.emit(new Values(item));

		return true;
	}

	@Override
	public boolean write(Collection<Data> data) throws Exception {
		for (Data item : data) {
			this.write(item);
		}
		return true;
	}

	/**
	 * Does currently nothing.
	 */
	@Override
	public void close() throws Exception {
		return;
	}

	public void setId(String id) {
		this.id = id;
	}

	@Override
	public String getId() {
		return this.id;
	}

	/**
	 * @see stream.io.Sink#init()
	 */
	@Override
	public void init() throws Exception {
	}
}
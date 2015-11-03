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
package stream;

import java.io.Serializable;

import stream.util.MD5;

/**
 * <p>
 * This class represents a simple source<->subscriber connection.
 * </p>
 * 
 * @author Christian Bockermann
 * 
 */
public class Subscription implements Serializable {

	/** The unique class ID */
	private static final long serialVersionUID = 8854045123111020160L;
	private final String source;
	private final String subscriber;
	private final String stream;

	public Subscription(String subscriber, String source) {
		this.source = source;
		this.subscriber = subscriber;
		this.stream = "default";
	}

	public Subscription(String subscriber, String source, String stream) {
		this.source = source;
		this.subscriber = subscriber;
		this.stream = stream;
	}

	public String source() {
		return source;
	}

	public String subscriber() {
		return subscriber;
	}

	public String stream() {
		return stream;
	}

	public String id() {
		return MD5.md5(source + "<->" + subscriber);
	}

	public String toString() {
		return "Subscription[" + subscriber + " ~> " + source + "_" + stream + "]";
	}

	public int hashCode() {
		return id().hashCode();
	}

	public boolean equals(Object o) {
		if (o == this)
			return true;

		if (o instanceof Subscription) {
			Subscription other = (Subscription) o;
			return id().equals(other.id());
		}

		return false;
	}
}
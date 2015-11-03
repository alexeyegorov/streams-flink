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

import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Tuple;

/**
 * @author chris
 */
public class QueueBolt extends AbstractBolt {

    /**
     * The unique class ID
     */
    private static final long serialVersionUID = -3206574886699994554L;

    static Logger log = LoggerFactory.getLogger(QueueBolt.class);
    OutputCollector output;

    /**
     * @param xmlConfig
     * @param uuid
     */
    public QueueBolt(String xmlConfig, String uuid) {
        super(xmlConfig, uuid);
    }

    /**
     * @see backtype.storm.task.IBolt#prepare(java.util.Map, backtype.storm.task.TopologyContext,
     * backtype.storm.task.OutputCollector)
     */
    @SuppressWarnings("rawtypes")
    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        output = collector;
        log.info("  > Preparing queue '{}' with collector '{}'", this.uuid, collector);
    }

    /**
     * @see backtype.storm.task.IBolt#execute(backtype.storm.tuple.Tuple)
     */
    @Override
    public void execute(Tuple input) {
        log.info("queue-bolt processing tuple {}", input);
        if (output != null) {
            log.info("   emitting tuple to stream...");
            output.emit(input.getValues());
        } else {
            log.info("   no output defined, discarding tuple...");
        }
    }
}

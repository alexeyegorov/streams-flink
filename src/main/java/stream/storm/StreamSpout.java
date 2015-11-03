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
import org.w3c.dom.Document;
import org.w3c.dom.Element;

import java.util.List;
import java.util.Map;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import stream.Data;
import stream.io.Stream;
import stream.runtime.setup.factory.ObjectFactory;
import stream.runtime.setup.factory.StreamFactory;
import stream.storm.config.StreamHandler.StreamFinder;
import stream.util.Variables;
import stream.util.XMLUtils;

/**
 * @author Christian Bockermann
 */
public class StreamSpout extends BaseRichSpout {

    /**
     * The unique class ID
     */
    private static final long serialVersionUID = -786482575770711600L;

    static Logger log = LoggerFactory.getLogger(StreamSpout.class);

    transient Stream stream;
    protected SpoutOutputCollector output;

    protected final Variables parameters;
    protected final String xml;
    protected final String id;

    public StreamSpout(String xml, String id, Map<String, String> params) throws Exception {
        log.debug("Creating spout for stream (params: {})", params);
        this.xml = xml;
        this.id = id;
        this.parameters = new Variables(params);
        stream = createStream();
    }

    protected Stream createStream() throws Exception {
        Document doc = XMLUtils.parseDocument(xml);
        List<Element> els = XMLUtils.findElements(doc, new StreamFinder(id));

        if (els.size() != 1) {
            throw new RuntimeException("Failed to locate 'stream' element for id '" + id + "'!");
        }

        Element el = els.get(0);
        ObjectFactory objectFactory = ObjectFactory.newInstance();
        Stream stream = StreamFactory.createStream(objectFactory, el, parameters);
        return stream;
    }

    /**
     * @see backtype.storm.spout.ISpout#open(java.util.Map, backtype.storm.task.TopologyContext,
     * backtype.storm.spout.SpoutOutputCollector)
     */
    @SuppressWarnings("rawtypes")
    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.output = collector;
        try {

            if (stream == null) {
                stream = createStream();
            }

            stream.init();
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException("Failed to open stream: "
                    + e.getMessage());
        }
    }

    /**
     * @see backtype.storm.spout.ISpout#nextTuple()
     */
    @Override
    public void nextTuple() {
        log.debug("nextTuple() called");
        try {
            Data item = stream.read();
            log.debug("read item: {}", item);
            if (item == null) {
                sleep(500);
            } else {
                log.debug("Emitting item as tuple...");
                output.emit(new Values(item));
            }
        } catch (Exception e) {
            log.error("Failed to read next item: {}", e.getMessage());
            if (log.isDebugEnabled())
                e.printStackTrace();
        }
    }

    protected void sleep(long ms) {
        try {
            Thread.sleep(ms);
        } catch (Exception e) {
            log.error("Error while sleep in StreamSpout with parameters: {}", parameters);
        }
    }

    /**
     * @see backtype.storm.topology.IComponent#declareOutputFields(backtype.storm
     * .topology.OutputFieldsDeclarer)
     */
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        log.debug("Declaring output-field 'stream.Data'");
        declarer.declare(new Fields("stream.Data"));
    }
}
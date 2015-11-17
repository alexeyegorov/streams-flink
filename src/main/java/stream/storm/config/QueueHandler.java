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
package stream.storm.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Element;

import backtype.storm.topology.BoltDeclarer;
import stream.StreamTopologyBuilder;
import stream.StreamTopology;
import stream.runtime.setup.factory.ObjectFactory;
import stream.storm.QueueBolt;

/**
 * @author chris
 */
public class QueueHandler extends ATopologyElementHandler {

    static Logger log = LoggerFactory.getLogger(QueueHandler.class);
    final String xml;

    /**
     * @param of
     */
    public QueueHandler(ObjectFactory of, String xml) {
        super(of);
        this.xml = xml;
    }

    /**
     * @see stream.storm.config.ConfigHandler#handles(org.w3c.dom.Element)
     */
    @Override
    public boolean handles(Element el) {
        return "queue".equalsIgnoreCase(el.getNodeName());
    }

    /**
     * @see stream.storm.config.ConfigHandler#handle(org.w3c.dom.Element, stream.StreamTopology,
     * StreamTopologyBuilder)
     */
    @Override
    public void handle(Element element, StreamTopology st, StreamTopologyBuilder builder) throws Exception {

        String id = element.getAttribute("id");
        if (id == null || id.trim().isEmpty()) {
            throw new Exception("Queue element does not specify 'id' attribute!");
        }

        QueueBolt bolt = new QueueBolt(xml, id);
        log.info("  >   Registering bolt (queue) '{}' with instance {}", id, bolt);

        // TODO: add number of copies for a bolt
        BoltDeclarer cur = builder.setBolt(id, bolt, 1);
        log.debug(" > Adding queue to stream-topology...");
        st.addBolt(id, cur);
    }
}

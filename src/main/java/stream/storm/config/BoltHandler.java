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

import org.apache.flink.storm.api.FlinkTopologyBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Element;

import java.util.List;
import java.util.Map;

import backtype.storm.topology.BoltDeclarer;
import backtype.storm.topology.IBasicBolt;
import backtype.storm.topology.IRichBolt;
import flink.StreamTopologyBuilder;
import stream.StreamTopology;
import stream.runtime.setup.factory.ObjectFactory;

/**
 * @author chris
 */
public class BoltHandler extends ATopologyElementHandler {

    static Logger log = LoggerFactory.getLogger(BoltHandler.class);

    public BoltHandler(ObjectFactory of) {
        super(of);
    }

    /**
     * @see stream.storm.config.ConfigHandler#handles(org.w3c.dom.Element)
     */
    @Override
    public boolean handles(Element el) {
        String name = el.getNodeName();
        return name.equalsIgnoreCase("flink:bolt");
    }

    /**
     * @see stream.storm.config.ConfigHandler#handle(org.w3c.dom.Element, stream.StreamTopology,
     * flink.StreamTopologyBuilder)
     */
    @Override
    public void handle(Element el, StreamTopology st, StreamTopologyBuilder builder)
            throws Exception {

        if (!handles(el))
            return;

        String id = el.getAttribute("id");
        if (id == null)
            throw new Exception("Element '" + el.getNodeName()
                    + "' is missing an 'id' attribute!");

        String className = el.getAttribute("class");
        Map<String, String> params = objectFactory.getAttributes(el);

        log.info("  > Found '{}' definition, with class: {}", el.getNodeName(), className);
        log.info("  >   Parameters are: {}", params);

        params = st.getVariables().expandAll(params);
        log.info("  >   Expanded parameters: {}", params);

        // log.debug(
        // "Creating direct bolt-instance for class '{}', params: {}",
        // className, params);
        log.info("  >   Creating bolt-instance from class {}, parameters: {}", className, params);

        Object obj = objectFactory.create(className, params,
                ObjectFactory.createConfigDocument(el));

        BoltDeclarer boltDeclarer = null;

        //TODO: add number of copies for a bolt
        if (obj instanceof IRichBolt) {
            IRichBolt bolt = (IRichBolt) obj;
            log.info("  > Registering bolt '{}' with instance {}", id, bolt);
            boltDeclarer = builder.setBolt(id, bolt);
        }

        if (obj instanceof IBasicBolt) {
            IBasicBolt bolt = (IBasicBolt) obj;
            log.info("  > Registering bolt '{}' with instance {}", id, bolt);
            boltDeclarer = builder.setBolt(id, bolt);
        }

        if (boltDeclarer == null) {
            log.debug("Bolt-class '{}' does not implement supported interface " +
                    "(only IRichBolt/IBasicBolt are supported)!", className);
            throw new Exception("Bolt-class does not implement supported interface " +
                    "(only IRichBolt/IBasicBolt are supported)!");
        }

        BoltDeclarer cur = boltDeclarer;
        List<String> inputs = getInputNames(el);
        if (!inputs.isEmpty()) {
            for (String input : inputs) {
                if (!input.isEmpty()) {
                    log.info("  > Connecting bolt '{}' to shuffle-group '{}'",
                            id, input);
                    cur = cur.shuffleGrouping(input);
                }
            }
        } else {
            log.debug("No inputs defined for bolt '{}'!", id);
        }

        st.addBolt(id, cur);
    }
}

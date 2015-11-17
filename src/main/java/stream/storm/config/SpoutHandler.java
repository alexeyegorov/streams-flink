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

import java.util.Map;

import backtype.storm.topology.IRichSpout;
import stream.StreamTopologyBuilder;
import stream.StreamTopology;
import stream.runtime.setup.factory.ObjectFactory;
import stream.storm.Constants;

/**
 * @author chris
 */
public class SpoutHandler extends ATopologyElementHandler {

    static Logger log = LoggerFactory.getLogger(SpoutHandler.class);

    /**
     * @param of
     */
    public SpoutHandler(ObjectFactory of) {
        super(of);
    }

    /**
     * @see stream.storm.config.ConfigHandler#handles(org.w3c.dom.Element)
     */
    @Override
    public boolean handles(Element el) {
        if (el == null)
            return false;

        String name = el.getNodeName();
        return "flink:spout".equalsIgnoreCase(name);
    }

    /**
     * @see stream.storm.config.ConfigHandler#handle(org.w3c.dom.Element, stream.StreamTopology,
     * StreamTopologyBuilder)
     */
    @Override
    public void handle(Element el, StreamTopology st, StreamTopologyBuilder builder)
            throws Exception {

        if (!handles(el)) {
            return;
        }

        String id = el.getAttribute(Constants.ID);
        if (id == null) {
            throw new Exception("Element '" + el.getNodeName() + "' is missing an 'id' attribute!");
        }

        String className = el.getAttribute("class");
        Map<String, String> params = objectFactory.getAttributes(el);

        log.info("  > Found '{}' definition, with class: {}", el.getNodeName(), className);
        log.info("  >   Parameters are: {}", params);

        params = st.getVariables().expandAll(params);
        log.info("  >   Expanded parameters: {}", params);

        log.info("  >   Creating spout-instance from class {}, parameters: {}", className, params);
        IRichSpout bolt = (IRichSpout) objectFactory.create(className, params,
                ObjectFactory.createConfigDocument(el));

        log.info("  > Registering spout '{}' with instance {}", id, bolt);

        //TODO: add number of copies for a spout (not just 2)

        st.addSpout(id, builder.setSpout(id, bolt, 2));
    }
}
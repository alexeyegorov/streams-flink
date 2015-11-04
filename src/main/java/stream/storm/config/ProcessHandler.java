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

import backtype.storm.topology.BoltDeclarer;
import stream.StreamTopology;
import stream.Subscription;
import stream.runtime.setup.factory.ObjectFactory;
import stream.storm.Constants;
import stream.storm.ProcessBolt;

/**
 * @author chris
 */
public class ProcessHandler extends ATopologyElementHandler {

    static Logger log = LoggerFactory.getLogger(ProcessHandler.class);

    final String xml; // the xml string (config)

    /**
     * @param of
     */
    public ProcessHandler(ObjectFactory of, String xml) {
        super(of);
        this.xml = xml;
    }

    /**
     * @see stream.storm.config.ConfigHandler#handles(org.w3c.dom.Element)
     */
    @Override
    public boolean handles(Element el) {
        String name = el.getNodeName();
        return name.equalsIgnoreCase("process");
    }

    /**
     * @see stream.storm.config.ConfigHandler#handle(org.w3c.dom.Element, stream.StreamTopology,
     * org.apache.flink.storm.api.FlinkTopologyBuilder)
     */
    @Override
    public void handle(Element el, StreamTopology st, FlinkTopologyBuilder builder) throws Exception {

        if (el.getNodeName().equalsIgnoreCase("process")) {
            String id = el.getAttribute(Constants.ID);
            if (id == null || id.trim().isEmpty()) {
                log.error("No 'id' attribute defined in process element (class: '{}')",
                        el.getAttribute("class"));
                throw new Exception("Missing 'id' attribute for process element!");
            }

            log.info("  > Creating process-bolt with id '{}'", id);

            String copies = el.getAttribute("copies");
            Integer workers = 1;
            if (copies != null && !copies.isEmpty()) {
                try {
                    workers = Integer.parseInt(copies);
                } catch (Exception e) {
                    throw new RuntimeException("Invalid number of copies '" + copies + "' specified!");
                }
            }

            ProcessBolt bolt = new ProcessBolt(xml, id, st.getVariables());
            log.info("  >   Registering bolt (process) '{}' with instance {}", id, bolt);

            List<String> inputs = getInputNames(el);
            BoltDeclarer cur = builder.setBolt(id, bolt, workers);
            if (!inputs.isEmpty()) {
                for (String in : inputs) {
                    if (!in.isEmpty()) {
                        //
                        // if 'in' is reference to a process/bolt
                        //

                        //
                        // else
                        //
                        log.info("  >   Connecting bolt '{}' to non-group '{}'", id, in);
                        st.addSubscription(new Subscription(id, in));
                        // cur = cur.noneGrouping(in);
                    }
                }
            } else {
                log.warn("No input defined for process '{}'!", id);
            }
            st.addBolt(id, cur);

            for (Subscription subscription : bolt.getSubscriptions()) {
                log.info("Adding subscription:  {}", subscription);
                st.addSubscription(subscription);
            }
        }
    }
}
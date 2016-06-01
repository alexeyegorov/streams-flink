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
package flink;

import org.apache.storm.Config;
import org.apache.storm.topology.TopologyBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

import java.io.File;
import java.net.URL;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

import stream.StreamTopology;
import stream.runtime.StreamRuntime;
import stream.Constants;
import stream.util.XMLUtils;

import static storm.deploy.handleArgs;

/**
 * Main method to run deployment process of flink topology that is transformed from streams
 * configuration in XML format into a storm topology.
 *
 * @author chris, alexey
 */
public class deploy {

    static Logger log = LoggerFactory.getLogger(deploy.class);

    /**
     * @param args
     */
    public static void main(String[] args) {
        try {
            stream.runtime.StreamRuntime.loadUserProperties();
            StreamRuntime.setupLogging();
            final Properties properties = new Properties();
            properties.putAll(System.getProperties());
            final List<String> params = handleArgs(args, properties);

            if (params.isEmpty()) {
                System.err.println("You need to specify an XML configuration!");
                System.exit(-1);
            }

            Config config = storm.deploy.readConfiguration(properties);

            URL url = new File(params.get(0)).toURI().toURL();
            log.info("Creating topology with config from '{}'", url);

            Document doc = XMLUtils.parseDocument(url.openStream());
            Element root = doc.getDocumentElement();
            String id = root.getAttribute("id");
            log.info("Container/topology ID is: '{}'", id);

            // retrieve number of workers to be used
            String numWorkers = root.getAttribute(Constants.NUM_WORKERS);
            if (!numWorkers.equals("")) {
                log.info("Topology should use {} workers.", numWorkers);
                int workers = Integer.valueOf(numWorkers);
                config.setNumWorkers(workers);
            }

            TopologyBuilder stormBuilder = new TopologyBuilder();
            StreamTopology topology = StreamTopology.build(doc, stormBuilder);

            String name = id;
            if (id == null || id.trim().isEmpty()) {
                name = UUID.randomUUID().toString().toLowerCase();
            }

            log.info("Submitting topology '{}'", name);
//            FlinkSubmitter.submitTopology(name, config, FlinkTopology.createTopology(stormBuilder));

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
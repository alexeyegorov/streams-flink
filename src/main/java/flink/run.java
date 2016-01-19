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

import org.apache.flink.storm.api.FlinkTopology;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;

import java.io.File;
import java.net.URL;
import java.util.List;
import java.util.Properties;

import backtype.storm.Config;
import backtype.storm.topology.TopologyBuilder;
import stream.DocumentEncoder;
import stream.StreamTopology;
import stream.StreamTopologyBuilder;
import stream.util.XMLUtils;

import static storm.deploy.handleArgs;

/**
 * Transforms streams XML configuration to a valid flink topology, start local flink cluster and
 * deploys created flink topology to the local cluster.
 *
 * @author chris
 */
public class run {

    static Logger log = LoggerFactory.getLogger(run.class);
    public final static String UUID_ATTRIBUTE = "id";

    /**
     * Method to start cluster and run XML configuration as flink topology on it while setting the
     * maximum running time to Long.MAX_VALUE.
     *
     * @param url path to XML configuration
     */
    public static void main(URL url) throws Exception {
        main(url, Long.MAX_VALUE);
    }

    /**
     * Parse XML configuration, create flink topology out of it and run it for some given time.
     *
     * @param url  path to the XML configuration
     * @param time maximum time for a cluster to run
     */
    public static void main(URL url, Long time) throws Exception {

        stream.runtime.StreamRuntime.loadUserProperties();

        System.setProperty("rlog.host", "127.0.0.1");
        System.setProperty("rlog.token", "ab09cfe1d60b602cb7600b5729da939f");

        StreamTopologyBuilder.ShutdownHook shutdown = new StreamTopologyBuilder.ShutdownHook();
        Runtime.getRuntime().addShutdownHook(shutdown);

        String xml = storm.run.createIDs(url.openStream());

        Document doc = XMLUtils.parseDocument(xml);
        doc = XMLUtils.addUUIDAttributes(doc, UUID_ATTRIBUTE);

        log.info("Encoding document...");
        String enc = DocumentEncoder.encodeDocument(doc);
        log.info("Arg will be:\n{}", enc);

        Document decxml = DocumentEncoder.decodeDocument(enc);
        log.info("Decoded XML is: {}", XMLUtils.toString(decxml));

        if (enc == null) {
            return;
        }

        Config conf = new Config();
        conf.setDebug(false);
//        conf.setNumWorkers(4);
//        conf.setMaxTaskParallelism(4);

        // create right stream topology
        TopologyBuilder stormBuilder = new TopologyBuilder();
        StreamTopology st = StreamTopology.build(doc, stormBuilder);
        log.info("Creating stream-topology...");
        FlinkTopology topology = FlinkTopology.createTopology(stormBuilder);
//        topology.setParallelism(2);

        // start local cluster and run created topology on it
        StreamTopologyBuilder.runOnLocalCluster(topology, conf, time);
    }

    /**
     * Main method to start cluster and run XML configuration as flink topology on it.
     *
     * @param args the first argument must be XML configuration
     */
    public static void main(String[] args) throws Exception {

        final Properties p = new Properties();
        List<String> params = handleArgs(args, p);

        if (params.isEmpty()) {
            System.err.println("You need to specify an XML configuration!");
            System.exit(-1);
        }

        stream.runtime.StreamRuntime.setupLogging();

        File file = new File(params.get(0));

        long timeValue = Long.MAX_VALUE;
        if (params.size() > 1) {
            try {
                timeValue = Long.parseLong(params.get(1));
            } catch (NumberFormatException nfe) {
                log.info("Second parameter should be a long number, using Long.MAX_VALUE.");
            }
        }
        main(file.toURI().toURL(), timeValue);
    }

}
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

import org.apache.flink.storm.api.FlinkLocalCluster;
import org.apache.flink.storm.api.FlinkTopology;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import java.io.File;
import java.io.InputStream;
import java.io.StringWriter;
import java.net.URL;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.transform.Result;
import javax.xml.transform.Source;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import backtype.storm.Config;
import backtype.storm.utils.Utils;
import stream.DocumentEncoder;
import stream.StreamTopology;
import stream.util.XMLUtils;

/**
 * Transforms streams XML configuration to a valid flink topology, start local flink cluster and
 * deploys created flink topology to the local cluster.
 *
 * @author chris
 */
public class run {

    static Logger log = LoggerFactory.getLogger(run.class);
    public final static String UUID_ATTRIBUTE = "id";
    private static FlinkLocalCluster localCluster;

    /**
     * Add to each attribute (processor, later bolt) an ID.
     *
     * @param element xml element
     */
    public static void addUUIDAttributes(Element element) {

        String theId = element.getAttribute("id");
        if (theId == null || theId.trim().isEmpty()) {
            UUID id = UUID.randomUUID();
            element.setAttribute(UUID_ATTRIBUTE, id.toString());
        }

        NodeList list = element.getChildNodes();
        for (int i = 0; i < list.getLength(); i++) {
            Node node = list.item(i);
            if (node.getNodeType() == Node.ELEMENT_NODE) {
                addUUIDAttributes((Element) node);
            }
        }
    }

    /**
     * Create IDs for each processor (bolt).
     *
     * @param in input stream (xml file)
     * @return input stream as String with added IDs
     */
    public static String createIDs(InputStream in) throws Exception {

        // parse input stream to a document (xml)
        DocumentBuilder builder = DocumentBuilderFactory.newInstance().newDocumentBuilder();
        Document doc = builder.parse(in);

        // add IDs
        addUUIDAttributes(doc.getDocumentElement());

        // write document to string
        Transformer trans = TransformerFactory.newInstance().newTransformer();
        Source source = new DOMSource(doc);
        StringWriter out = new StringWriter();
        Result output = new StreamResult(out);
        trans.transform(source, output);

        return out.toString();
    }


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

        ShutdownHook shutdown = new ShutdownHook();
        Runtime.getRuntime().addShutdownHook(shutdown);

        InputStream in = url.openStream();

        String xml = createIDs(in);

        Document doc = XMLUtils.parseDocument(xml);
        doc = XMLUtils.addUUIDAttributes(doc, UUID_ATTRIBUTE);

        log.info("Encoding document...");
        String enc = DocumentEncoder.encodeDocument(doc);
        log.info("Arg will be:\n{}", enc);

        Document decxml = DocumentEncoder.decodeDocument(enc);
        log.info("Decoded XML is: {}", XMLUtils.toString(decxml));

        if (enc == null)
            return;

        Config conf = new Config();
        conf.setDebug(false);

        StreamTopology st = StreamTopology.create(doc);

        log.info("Creating stream-topology...");

        FlinkTopology storm = st.createTopology();

        log.info("Starting local cluster...");
        FlinkLocalCluster cluster = startLocalCluster();

        log.info("########################################################################");
        log.info("submitting topology...");
        String topId = System.getProperty("id", UUID.randomUUID().toString());
        cluster.submitTopology(topId, conf, storm);
        log.info("########################################################################");

        log.info("Topology submitted.");

        Utils.sleep(time);

        cluster.killTopology(topId);
    }

    public static FlinkLocalCluster getLocalCluster() {
        return localCluster;
    }

    public static FlinkLocalCluster startLocalCluster() {
        if (localCluster != null) {
            log.info("Local cluster {} already running...", localCluster);
            return localCluster;
        }

        localCluster = new FlinkLocalCluster();
        return localCluster;
    }

    public static void stopLocalCluster() {
        if (localCluster != null) {
            localCluster.shutdown();
        }
    }

    /**
     * Main method to start cluster and run XML configuration as flink topology on it.
     *
     * @param args the first argument must be XML configuration
     */
    public static void main(String[] args) throws Exception {

        final Properties p = new Properties();
        List<String> params = flink.deploy.handleArgs(args, p);

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

    public static class ShutdownHook extends Thread {

        private Set<String> topologies = new LinkedHashSet<String>();

        public void addTopology(String name) {
            topologies.add(name);
        }

        public void run() {

            if (flink.run.getLocalCluster() == null) {
                log.info("No local cluster started, nothing to shut down...");
                return;
            }

            for (String topo : topologies) {
                log.info("Killing topology '{}'", topo);
                flink.run.getLocalCluster().killTopology(topo);
            }

            log.info("Shutting down local cluster...");
            flink.run.stopLocalCluster();
        }
    }
}
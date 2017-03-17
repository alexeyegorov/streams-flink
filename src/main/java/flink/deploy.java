package flink;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import java.io.File;
import java.io.InputStream;
import java.io.StringWriter;
import java.net.URI;
import java.util.HashSet;
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

import stream.FlinkStreamTopology;
import stream.util.XMLUtils;

/**
 * Build and run Flink topology locally or deploy jar with this class as mainclass to your cluster.
 *
 * @author alexey
 */
public class deploy {


    static Logger log = LoggerFactory.getLogger(deploy.class);

    /**
     * Method to start cluster and run XML configuration as flink topology on it while setting the
     * maximum running time to Long.MAX_VALUE.
     *
     * @param url path to XML configuration
     */
    public static void main(InputStream url) throws Exception {
        main(url, Long.MAX_VALUE);
    }

    /**
     * Parse XML configuration, create flink topology out of it and run it for some given time.
     *
     * @param url  path to the XML configuration
     * @param time maximum time for a cluster to run
     */
    public static void main(InputStream url, Long time) throws Exception {
        String xml = createIDs(url);

        Document doc = XMLUtils.parseDocument(xml);

        log.info("Encoding document...");
        String enc = DocumentEncoder.encodeDocument(doc);
        log.info("Arg will be:\n{}", enc);

        Document decxml = DocumentEncoder.decodeDocument(enc);
        log.info("Decoded XML is: {}", XMLUtils.toString(decxml));

        FlinkStreamTopology flinkStreamTopology = new FlinkStreamTopology(doc);

        if (flinkStreamTopology.createTopology()) {
            flinkStreamTopology.executeTopology();
        } else {
            log.info("Do not execute as there were errors while building the topology.");
        }
    }

    /**
     * Entry main method that extracts file path to XML configuration.
     *
     * @param args list of parameters
     */
    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            log.error("Missing file path to XML configuration of streams job to run.");
            return;
        }

        String url = args[0];
        if (url.startsWith("hdfs:")) {
            DistributedFileSystem dfs = new DistributedFileSystem();
            String pathWithoutHDFS = url.substring("hdfs://".length());
            URI uri = new URI("hdfs://" + pathWithoutHDFS.substring(0, pathWithoutHDFS.indexOf("/")));
            Configuration conf = new Configuration();
            conf.set("fs.defaultFS", "hdfs://" + uri.toString());
            dfs.initialize(uri, conf);
            Path path = new Path(url);
            if (!dfs.exists(path)) {
                log.error("Path to XML configuration is not valid: {}", path.toString());
                return;
            }
            main(dfs.open(path));
        } else {
            File file = new File(url);
            if (!file.getAbsoluteFile().exists() || !file.exists()) {
                log.error("Path to XML configuration is not valid: {}", file.toString());
                return;
            }
            main(file.toURI().toURL().openStream());
        }
    }


    public final static String UUID_ATTRIBUTE = "id";

    final static Set<String> requiresID = new HashSet<>();

    static {
        requiresID.add("process");
        requiresID.add("stream");
        requiresID.add("queue");
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
     * Add to each attribute (processor, later bolt) an ID.
     *
     * @param element xml element
     */
    public static void addUUIDAttributes(Element element) {

        if (requiresID.contains(element.getTagName())) {
            String theId = element.getAttribute("id");
            log.info("   attribute '{}' for element '{}' is: " + theId, "id", element.getTagName());
            if (theId == null || theId.trim().isEmpty()) {
                UUID id = UUID.randomUUID();
                log.info("Adding UUID attribute to {}", element.getTagName());
                element.setAttribute(UUID_ATTRIBUTE, id.toString());
            }
        }

        NodeList list = element.getChildNodes();
        for (int i = 0; i < list.getLength(); i++) {
            Node node = list.item(i);
            if (node.getNodeType() == Node.ELEMENT_NODE) {
                addUUIDAttributes((Element) node);
            }
        }
    }
}

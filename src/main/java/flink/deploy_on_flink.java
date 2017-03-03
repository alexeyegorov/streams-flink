package flink;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;

import java.io.File;
import java.io.InputStream;
import java.net.URI;

import stream.FlinkStreamTopology;
import stream.util.XMLUtils;

/**
 * Build and run Flink topology locally or deploy jar with this class as mainclass to your cluster.
 *
 * @author alexey
 */
public class deploy_on_flink {


    static Logger log = LoggerFactory.getLogger(deploy_on_flink.class);

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
        stream.runtime.StreamRuntime.loadUserProperties();

        String xml = storm.run.createIDs(url);

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
}

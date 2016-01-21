package flink;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;

import java.io.File;
import java.io.IOException;
import java.net.URL;

import backtype.storm.Config;
import stream.DocumentEncoder;
import stream.StreamTopology;
import stream.util.XMLUtils;

/**
 * @author alexey
 */
public class deploy_on_flink {

    static Logger log = LoggerFactory.getLogger(deploy_on_flink.class);
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

//        System.setProperty("rlog.host", "127.0.0.1");
//        System.setProperty("rlog.token", "ab09cfe1d60b602cb7600b5729da939f");

//        StreamTopologyBuilder.ShutdownHook shutdown = new StreamTopologyBuilder.ShutdownHook();
//        Runtime.getRuntime().addShutdownHook(shutdown);

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
//        TopologyBuilder stormBuilder = new TopologyBuilder();

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        StreamTopology st = StreamTopology.create(doc, env);

//        log.info("Creating stream-topology...");
//        FlinkTopology topology = FlinkTopology.createTopology(stormBuilder);
////        topology.setParallelism(2);
//
//        // start local cluster and run created topology on it
//        StreamTopologyBuilder.runOnLocalCluster(topology, conf, time);
    }

        public static void main(String[] args) throws Exception {
            File file = new File(args[0]);
            main(file.toURI().toURL());
//            final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
//
//            DataSet<String> text = env.fromElements(
//                    "Who's there?",
//                    "I think I hear them. Stand, ho! Who's there?");
//
//            DataSet<Tuple2<String, Integer>> wordCounts = text
//                    .flatMap(new LineSplitter())
//                    .groupBy(0)
//                    .sum(1);
//
//            wordCounts.print();
//
//            env.execute("Word Count Example");
        }


//        public static class LineSplitter implements FlatMapFunction<String, Tuple2<String, Integer>> {
//            @Override
//            public void flatMap(String line, Collector<Tuple2<String, Integer>> out) {
//                for (String word : line.split(" ")) {
//                    out.collect(new Tuple2<String, Integer>(word, 1));
//                }
//            }
//        }
}

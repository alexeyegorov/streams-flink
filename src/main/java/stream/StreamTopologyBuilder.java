package stream;

import org.apache.flink.storm.api.FlinkLocalCluster;
import org.apache.flink.storm.api.FlinkTopology;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.UUID;

import backtype.storm.Config;
import backtype.storm.utils.Utils;

public class StreamTopologyBuilder {
    private static FlinkLocalCluster flinkCluster;

    static Logger log = LoggerFactory.getLogger(StreamTopologyBuilder.class);

    public static void stopLocalCluster() {
        flinkCluster.shutdown();
    }

    public static void submitTopology(String topId, Config conf, Object topology) {
        flinkCluster = new FlinkLocalCluster();
        try {
            flinkCluster.submitTopology(topId, conf, (FlinkTopology) topology);
        } catch (Exception e) {
            log.error("Flink local cluster couldn't have been started:\n{}",
                    Arrays.toString(e.getStackTrace()));
        }
    }

    public static void killTopology(String topo) {
        flinkCluster.killTopology(topo);
    }

    public static void runOnLocalCluster(Object topology, Config conf, Long time) {
        log.info("Starting local cluster...");
        String topId = System.getProperty("id", UUID.randomUUID().toString());

        log.info("########################################################################");
        log.info("submitting topology...");
        StreamTopologyBuilder.submitTopology(topId, conf, topology);
        log.info("########################################################################");

        log.info("Topology submitted.");

        Utils.sleep(time);

        StreamTopologyBuilder.killTopology(topId);
    }

    public static class ShutdownHook extends Thread {

        private Set<String> topologies = new LinkedHashSet<>();

        public void addTopology(String name) {
            topologies.add(name);
        }

        public void run() {

            if (flinkCluster == null) {
                log.info("No local cluster started, nothing to shut down...");
                return;

            }
            for (String topo : topologies) {
                log.info("Killing topology '{}'", topo);
                StreamTopologyBuilder.killTopology(topo);
            }

            log.info("Shutting down local cluster...");
            StreamTopologyBuilder.stopLocalCluster();
        }
    }

}

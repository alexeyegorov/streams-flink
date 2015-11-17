package flink;

import org.apache.flink.storm.api.FlinkLocalCluster;
import org.apache.flink.storm.api.FlinkTopology;
import org.apache.flink.storm.api.FlinkTopologyBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.Set;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.generated.StormTopology;
import backtype.storm.topology.BoltDeclarer;
import backtype.storm.topology.IBasicBolt;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.SpoutDeclarer;
import backtype.storm.topology.TopologyBuilder;
import stream.storm.AbstractBolt;
import stream.storm.StreamSpout;

public class StreamTopologyBuilder {
    private TopologyBuilder stormBuilder;
    private FlinkTopologyBuilder flinkBuilder;
    private static boolean isStorm = false;
    private static boolean isFlink = false;
    private static LocalCluster stormCluster;
    private static FlinkLocalCluster flinkCluster;

    static Logger log = LoggerFactory.getLogger(StreamTopologyBuilder.class);

    private StreamTopologyBuilder(String type) {
        if (type.equals("storm")) {
            stormBuilder = new TopologyBuilder();
            isStorm = true;
        } else if (type.equals("flink")) {
            flinkBuilder = new FlinkTopologyBuilder();
            isFlink = true;
        }
    }

    public static StreamTopologyBuilder createStormTopologyBuilder() {
        return new StreamTopologyBuilder("storm");
    }

    public static StreamTopologyBuilder createFlinkTopologyBuilder() {
        return new StreamTopologyBuilder("flink");
    }

    public Object createTopology() {
        return isStorm ? stormBuilder.createTopology() :
                isFlink ? flinkBuilder.createTopology() : null;
    }

    public BoltDeclarer setBolt(String id, AbstractBolt bolt, Integer workers) {
        return isStorm ? stormBuilder.setBolt(id, bolt, workers) :
                isFlink ? flinkBuilder.setBolt(id, bolt, workers) : null;
    }

    public BoltDeclarer setBolt(String id, IRichBolt bolt) {
        return isStorm ? stormBuilder.setBolt(id, bolt) :
                isFlink ? flinkBuilder.setBolt(id, bolt) : null;
    }

    public BoltDeclarer setBolt(String id, IBasicBolt bolt) {
        return isStorm ? stormBuilder.setBolt(id, bolt) :
                isFlink ? flinkBuilder.setBolt(id, bolt) : null;
    }

    public SpoutDeclarer setSpout(String id, IRichSpout bolt, int workers) {
        return isStorm ? stormBuilder.setSpout(id, bolt) :
                isFlink ? flinkBuilder.setSpout(id, bolt, workers) : null;
    }

    public SpoutDeclarer setSpout(String id, StreamSpout spout) {
        return setSpout(id, spout, 1);
    }

    public static Object getLocalCluster() {
        return isStorm ? stormCluster :
                isFlink ? flinkCluster : null;
    }

    public static void startLocalCluster() {
        Object localCluster = getLocalCluster();
        if (localCluster != null) {
            log.info("Local cluster {} already running...", localCluster);
        }

        if(isStorm){
            stormCluster = new LocalCluster();
        } else if(isFlink){
            flinkCluster = new FlinkLocalCluster();
        }
    }

    public static void stopLocalCluster() {
        Object localCluster = getLocalCluster();
        if (localCluster != null) {
            if (isStorm) {
                ((LocalCluster) localCluster).shutdown();
            } else if (isFlink) {
                ((FlinkLocalCluster) localCluster).shutdown();
            } else {
                //TODO log something
            }
        }
    }

    public static void submitTopology(String topId, Config conf, Object topology) {
        Object localCluster = getLocalCluster();
        if (localCluster != null) {
            if (isStorm) {
                ((LocalCluster) localCluster).submitTopology(topId, conf, (StormTopology) topology);
            } else if (isFlink) {
                try {
                    ((FlinkLocalCluster) localCluster).submitTopology(topId, conf, (FlinkTopology) topology);
                } catch (Exception e) {
                    log.error("Flink local cluster couldn't have been started:\n{}",
                            Arrays.toString(e.getStackTrace()));
                }
            } else {
                //TODO log something
            }
        }
    }

    public static void killTopology(String topo) {
        Object localCluster = getLocalCluster();
        if (localCluster != null) {
            if (isStorm) {
                ((LocalCluster) localCluster).killTopology(topo);
            } else if (isFlink) {
                ((FlinkLocalCluster) localCluster).killTopology(topo);
            } else {
                //TODO log something
            }
        }
    }

    public static class ShutdownHook extends Thread {

        private Set<String> topologies = new LinkedHashSet<>();

        public void addTopology(String name) {
            topologies.add(name);
        }

        public void run() {

            if (StreamTopologyBuilder.getLocalCluster() == null) {
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

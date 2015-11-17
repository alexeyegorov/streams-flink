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
package stream;

import org.apache.flink.storm.api.FlinkSubmitter;
import org.apache.flink.storm.api.FlinkTopology;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import backtype.storm.Config;
import backtype.storm.generated.StormTopology;
import backtype.storm.topology.BoltDeclarer;
import backtype.storm.topology.SpoutDeclarer;
import flink.StreamTopologyBuilder;
import stream.runtime.DependencyInjection;
import stream.runtime.setup.factory.ObjectFactory;
import stream.runtime.setup.handler.PropertiesHandler;
import stream.storm.Constants;
import stream.storm.config.BoltHandler;
import stream.storm.config.ConfigHandler;
import stream.storm.config.ProcessHandler;
import stream.storm.config.QueueHandler;
import stream.storm.config.SpoutHandler;
import stream.storm.config.StreamHandler;
import stream.util.Variables;
import stream.util.XMLUtils;

/**
 * @author Christian Bockermann &lt;christian.bockermann@udo.edu&gt;
 */
public class StreamTopology {

    static Logger log = LoggerFactory.getLogger(StreamTopology.class);

    public final StreamTopologyBuilder builder;
    public final Map<String, BoltDeclarer> bolts = new LinkedHashMap<>();
    public final Map<String, SpoutDeclarer> spouts = new LinkedHashMap<>();
    public final Variables variables = new Variables();

    final Set<Subscription> subscriptions = new LinkedHashSet<>();

    /**
     *
     * @param builder
     */
    private StreamTopology(StreamTopologyBuilder builder) {
        this.builder = builder;
    }

    public StreamTopologyBuilder getTopologyBuilder() {
        return builder;
    }

    public Variables getVariables() {
        return variables;
    }

    public void addSubscription(Subscription sub) {
        subscriptions.add(sub);
    }

    /**
     * Creates a new instance of a StreamTopology based on the given document. This also creates a
     * standard TopologyBuilder to build the associated Storm Topology.
     *
     * @param doc The DOM document that defines the topology.
     */
    public static StreamTopology create(Document doc) throws Exception {
        return build(doc, StreamTopologyBuilder.createFlinkTopologyBuilder());
    }

    /**
     * Creates a new instance of a StreamTopology based on the given document and using the
     * specified TopologyBuilder.
     */
    public static StreamTopology build(Document doc, StreamTopologyBuilder builder) throws Exception {

        final StreamTopology st = new StreamTopology(builder);

        doc = XMLUtils.addUUIDAttributes(doc, Constants.UUID_ATTRIBUTE);

        String xml = XMLUtils.toString(doc);
        DependencyInjection dependencies = new DependencyInjection();

        // a map of pre-defined inputs, i.e. input-names => uuids
        // to catch the case when processes read from queues that have
        // not been explicitly defined (i.e. 'linking bolts')
        //
        // Map<String, String> streams = new LinkedHashMap<String, String>();
        ObjectFactory of = ObjectFactory.newInstance();

        try {
            PropertiesHandler handler = new PropertiesHandler();
            handler.handle(null, doc, st.getVariables(), dependencies);
            of.addVariables(st.getVariables());

            if (log.isDebugEnabled()) {
                log.debug("########################################################################");
                log.debug("Found properties: {}", st.getVariables());
                for (String key : st.getVariables().keySet()) {
                    log.debug("   '{}' = '{}'", key, st.getVariables().get(key));
                }
                log.debug("########################################################################");
            }
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(-1);
        }

        List<ConfigHandler> handlers = new ArrayList<ConfigHandler>();
        handlers.add(new SpoutHandler(of));
        handlers.add(new QueueHandler(of, xml));
        handlers.add(new StreamHandler(of, xml));
        handlers.add(new BoltHandler(of));
        handlers.add(new ProcessHandler(of, xml));

        NodeList list = doc.getDocumentElement().getChildNodes();
        int length = list.getLength();

        for (ConfigHandler handler : handlers) {

            for (int i = 0; i < length; i++) {
                Node node = list.item(i);
                if (node.getNodeType() == Node.ELEMENT_NODE) {
                    Element el = (Element) node;

                    if (handler.handles(el)) {
                        log.info("--------------------------------------------------------------------------------");
                        log.info("Handling element '{}'", node.getNodeName());
                        handler.handle(el, st, builder);
                        log.info("--------------------------------------------------------------------------------");
                    }
                }
            }
        }

        //
        // resolve subscriptions
        //
        Iterator<Subscription> it = st.subscriptions.iterator();
        log.info("--------------------------------------------------------------------------------");
        while (it.hasNext()) {
            Subscription s = it.next();
            log.info("   {}", s);
        }
        log.info("--------------------------------------------------------------------------------");
        it = st.subscriptions.iterator();

        while (it.hasNext()) {
            Subscription subscription = it.next();
            log.info("Resolving subscription {}", subscription);

            BoltDeclarer subscriber = st.bolts.get(subscription.subscriber());
            if (subscriber != null) {
                String source = subscription.source();
                String stream = subscription.stream();
                if (stream.equals("default")) {
                    log.info("connecting '{}' to shuffle-group '{}'", subscription.subscriber(), source);
                    subscriber.shuffleGrouping(source);
                } else {
                    log.info("connecting '{}' to shuffle-group '{}:" + stream + "'", subscription.subscriber(), source);
                    subscriber.shuffleGrouping(source, stream);
                }
                it.remove();
            } else {
                log.error("No subscriber found for id '{}'", subscription.subscriber());
            }
        }

        if (!st.subscriptions.isEmpty()) {
            log.info("Unresolved subscriptions: {}", st.subscriptions);
            throw new Exception("Found " + st.subscriptions.size() + " unresolved subscription references!");
        }

        return st;
    }

    public void addBolt(String id, BoltDeclarer bolt) {
        bolts.put(id, bolt);
    }

    public void addSpout(String id, SpoutDeclarer spout) {
        spouts.put(id, spout);
    }

    /**
     * This method creates a new instance of type StormTopology based on the topology that has been
     * created from the DOM document.
     */
    public FlinkTopology createFlinkTopology() {
        return (FlinkTopology) builder.createTopology();
    }

    public StormTopology createStormTopology() {
        return (StormTopology) builder.createTopology();
    }
}
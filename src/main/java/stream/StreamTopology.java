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

import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import backtype.storm.topology.BoltDeclarer;
import backtype.storm.topology.SpoutDeclarer;
import backtype.storm.topology.TopologyBuilder;
import flink.FlinkProcessList;
import flink.FlinkQueue;
import flink.config.FlinkConfigHandler;
import flink.config.ProcessListHandler;
import flink.config.QueueHandler;
import flink.config.SourceHandler;
import stream.runtime.DependencyInjection;
import stream.runtime.setup.factory.ObjectFactory;
import stream.runtime.setup.handler.PropertiesHandler;
import stream.storm.Constants;
import stream.storm.config.BoltHandler;
import stream.storm.config.ConfigHandler;
import stream.storm.config.ProcessHandler;
import stream.storm.config.SpoutHandler;
import stream.storm.config.StreamHandler;
import stream.util.Variables;
import stream.util.XMLUtils;

/**
 * @author Christian Bockermann &lt;christian.bockermann@udo.edu&gt;
 */
public class StreamTopology {

    static Logger log = LoggerFactory.getLogger(StreamTopology.class);

    public final TopologyBuilder builder;
    public final Map<String, BoltDeclarer> bolts = new LinkedHashMap<>();
    public final Map<String, SpoutDeclarer> spouts = new LinkedHashMap<>();
    public final Variables variables = new Variables();

    final Set<Subscription> subscriptions = new LinkedHashSet<>();

    public List<FlinkQueue> flinkQueues = new ArrayList<>(0);

    /**
     * Create StreamTopology without topology builder (for flink purpose)
     */
    private StreamTopology() {
        this.builder = null;
    }

    /**
     * @param builder topology builder provided by Apache Storm
     */
    private StreamTopology(TopologyBuilder builder) {
        this.builder = builder;
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
    /**
     * Creates a new instance of a StreamTopology based on the given document and using the
     * specified TopologyBuilder.
     */
    public static StreamTopology create(Document doc, StreamExecutionEnvironment env)
            throws Exception {

        final StreamTopology st = new StreamTopology();

        // add unique IDs
        doc = XMLUtils.addUUIDAttributes(doc, Constants.UUID_ATTRIBUTE);

        // search for 'application' or 'container' tag and extract its ID
        String appId = "application:" + UUID.randomUUID().toString();
        NodeList nodeList = doc.getElementsByTagName("application");
        if (nodeList.getLength() < 1) {
            nodeList = doc.getElementsByTagName("container");
        }
        if (nodeList.getLength() > 1) {
            log.error("More than 1 application node.");
        } else {
            appId = nodeList.item(0).getAttributes().getNamedItem("id").getNodeValue();
        }

        // save application ID
        st.getVariables().put("application.id", appId);

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

        ArrayList<FlinkConfigHandler> handlers = new ArrayList<>();
        SourceHandler sourceHandler = new SourceHandler(of);
//        handlers.add();
        handlers.add(new ProcessListHandler(of, xml));
        QueueHandler queueHandler = new QueueHandler(of);
//        handlers.add();

//        handlers.add(new SpoutHandler(of));
//        handlers.add(new StreamHandler(of, xml));
//        handlers.add(new BoltHandler(of));
//        handlers.add(new ProcessHandler(of, xml));

        NodeList list = doc.getDocumentElement().getChildNodes();
        int length = list.getLength();

        // create stream sources (multiple are possible)
        NodeList streamList = doc.getDocumentElement().getElementsByTagName("stream");
        //TODO use something more simple?!
        HashMap<String, DataStream<Data>> sources = new HashMap<>(streamList.getLength());
        for (int is = 0; is < streamList.getLength(); is++) {
            Element item = (Element) streamList.item(is);
            if (sourceHandler.handles(item)) {
                // name of the source
                String id = item.getAttribute("id");

                // handle the source and create data stream for it
                sourceHandler.handle(item, st, env);
                DataStream<Data> source = env.addSource(sourceHandler.getFunction());

                // put this source into the hashmap
                sources.put(id, source);
            } else {
                log.debug("Source handler doesn't handle {}", item.toString());
            }

        }

        // create all possible queues
        NodeList queueList = doc.getDocumentElement().getElementsByTagName("queue");
        for (int iq = 0; iq < queueList.getLength(); iq++) {
            Element element = (Element) queueList.item(iq);
            if (queueHandler.handles(element)) {
                queueHandler.handle(element, st, env);
                FlinkQueue flinkQueue = (FlinkQueue) queueHandler.getFunction();
                st.flinkQueues.add(flinkQueue);
            }
        }

        for (FlinkConfigHandler handler : handlers) {

            for (int i = 0; i < length; i++) {
                Node node = list.item(i);
                if (node.getNodeType() == Node.ELEMENT_NODE) {
                    Element el = (Element) node;

                    if (handler.handles(el)) {
                        log.info("--------------------------------------------------------------------------------");
                        log.info("Handling element '{}'", node.getNodeName());
                        handler.handle(el, st, env);
                        String input = el.getAttribute("input");
                        log.info("--------------------------------------------------------------------------------");
//                        if (SourceHandler.class.isInstance(handler)){
//                            dataStream = env.addSource(((SourceHandler) handler).getFunction());
//                        } else
                        if (ProcessListHandler.class.isInstance(handler)) {
                            FlinkProcessList function = ((ProcessListHandler) handler).getFunction();
                            final List<String> outputQueues = function.getListOfOutputQueues();
                            DataStream<Data> dataStream = sources
                                    .get(input)
                                    .map(function);
                            //TODO split the datastream and save the splits into 'sources' hashmap
                            SplitStream<Data> split = dataStream.split(new OutputSelector<Data>() {
                                @Override
                                public Iterable<String> select(Data data) {
                                    List<String> queues = new ArrayList<>(outputQueues.size());
                                    if (data.containsKey("flink.queue")) {
                                        String outputQueue = (String) data.get("flink.queue");
                                        for (String queue : outputQueues) {
                                            if (queue.equals(outputQueue)) {
                                                queues.add(queue);
                                            }
                                        }
                                    }
                                    return queues;
                                }
                            });
                            for (String queue : outputQueues) {
                                sources.put(queue, split.select(queue));
                            }
                            sources.put(input, dataStream);
                        }
                    }
                }
            }
        }

//        if (dataStream == null){
//            log.debug("Process has not been initialized.\nQuitting...");
//            System.exit(-1);
//        }
        //dataStream.print();

        env.execute();

        //
        // resolve subscriptions
        //
//        Iterator<Subscription> it = st.subscriptions.iterator();
//        log.info("--------------------------------------------------------------------------------");
//        while (it.hasNext()) {
//            Subscription s = it.next();
//            log.info("   {}", s);
//        }
//        log.info("--------------------------------------------------------------------------------");
//        it = st.subscriptions.iterator();
//
//        while (it.hasNext()) {
//            Subscription subscription = it.next();
//            log.info("Resolving subscription {}", subscription);
//
//            BoltDeclarer subscriber = st.bolts.get(subscription.subscriber());
//            if (subscriber != null) {
//                String source = subscription.source();
//                String stream = subscription.stream();
//                if (stream.equals("default")) {
//                    log.info("connecting '{}' to shuffle-group '{}'", subscription.subscriber(), source);
//                    subscriber.shuffleGrouping(source);
//                } else {
//                    log.info("connecting '{}' to shuffle-group '{}:" + stream + "'", subscription.subscriber(), source);
//                    subscriber.shuffleGrouping(source, stream);
//                }
//                it.remove();
//            } else {
//                log.error("No subscriber found for id '{}'", subscription.subscriber());
//            }
//        }
//
//        if (!st.subscriptions.isEmpty()) {
//            log.info("Unresolved subscriptions: {}", st.subscriptions);
//            throw new Exception("Found " + st.subscriptions.size() + " unresolved subscription references!");
//        }

        return st;
    }

    /**
     * Creates a new instance of a StreamTopology based on the given document and using the
     * specified TopologyBuilder.
     */
    public static StreamTopology build(Document doc, TopologyBuilder builder) throws Exception {

        final StreamTopology st = new StreamTopology(builder);

        doc = XMLUtils.addUUIDAttributes(doc, Constants.UUID_ATTRIBUTE);
        String appId = "application:" + UUID.randomUUID().toString();
        NodeList nodeList = doc.getElementsByTagName("application");
        if (nodeList.getLength() < 1) {
            nodeList = doc.getElementsByTagName("container");
        }
        if (nodeList.getLength() > 1) {
            log.error("More than 1 application node.");
        } else {
            appId = nodeList.item(0).getAttributes().getNamedItem("id").getNodeValue();
        }
        st.getVariables().put("application.id", appId);
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

        List<ConfigHandler> handlers = new ArrayList<>();
        handlers.add(new SpoutHandler(of));
        handlers.add(new stream.storm.config.QueueHandler(of, xml));
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
}
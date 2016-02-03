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
import java.util.List;
import java.util.UUID;

import flink.functions.FlinkProcessList;
import flink.functions.FlinkQueue;
import flink.config.FlinkConfigHandler;
import flink.config.ProcessListHandler;
import flink.config.QueueHandler;
import flink.config.SourceHandler;
import stream.runtime.DependencyInjection;
import stream.runtime.setup.factory.ObjectFactory;
import stream.runtime.setup.handler.PropertiesHandler;
import stream.storm.Constants;
import stream.util.Variables;
import stream.util.XMLUtils;

/**
 * @author alexey
 */
public class FlinkStreamTopology {
    static Logger log = LoggerFactory.getLogger(FlinkStreamTopology.class);

    public final Variables variables = new Variables();

    /**
     * List of queues used for inter-process communication.
     */
    public List<FlinkQueue> flinkQueues = new ArrayList<>(0);
    public StreamExecutionEnvironment env;


    public Variables getVariables() {
        return variables;
    }

    /**
     * Creates a new instance of a StreamTopology based on the given document and using
     * stream execution environment
     *
     * @param doc The DOM document that defines the topology.
     */
    public static FlinkStreamTopology create(Document doc) throws Exception {

        final FlinkStreamTopology st = new FlinkStreamTopology();
        st.env = StreamExecutionEnvironment.getExecutionEnvironment();

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

        ObjectFactory of = ObjectFactory.newInstance();

        st.getVariables().addVariables(StreamTopology.handleProperties(doc, st.getVariables()));

        ArrayList<FlinkConfigHandler> handlers = new ArrayList<>();
        handlers.add(new ProcessListHandler(of, xml));

        // create stream sources (multiple are possible)
        NodeList streamList = doc.getDocumentElement().getElementsByTagName("stream");
        if (streamList.getLength() < 1) {
            log.debug("At least 1 stream source has to be defined.");
            return null;
        }

        SourceHandler sourceHandler = new SourceHandler(of);
        //TODO use something more simple?!
        HashMap<String, DataStream<Data>> sources = new HashMap<>(streamList.getLength());
        for (int is = 0; is < streamList.getLength(); is++) {
            Element item = (Element) streamList.item(is);
            if (sourceHandler.handles(item)) {
                // name of the source
                String id = item.getAttribute("id");

                // handle the source and create data stream for it
                sourceHandler.handle(item, st, st.env);
                DataStream<Data> source = st.env.addSource(sourceHandler.getFunction());

                // put this source into the hashmap
                sources.put(id, source);
                log.info("'{}' added as stream source.", id);
            } else {
                log.debug("Source handler doesn't handle {}", item.toString());
            }

        }

        // create all possible queues
        NodeList queueList = doc.getDocumentElement().getElementsByTagName("queue");
        QueueHandler queueHandler = new QueueHandler(of);
        for (int iq = 0; iq < queueList.getLength(); iq++) {
            Element element = (Element) queueList.item(iq);
            if (queueHandler.handles(element)) {
                queueHandler.handle(element, st, st.env);
                FlinkQueue flinkQueue = (FlinkQueue) queueHandler.getFunction();
                st.flinkQueues.add(flinkQueue);
            }
        }

        NodeList list = doc.getDocumentElement().getChildNodes();
        int length = list.getLength();
        for (FlinkConfigHandler handler : handlers) {

            for (int i = 0; i < length; i++) {
                Node node = list.item(i);
                if (node.getNodeType() == Node.ELEMENT_NODE) {
                    final Element el = (Element) node;

                    if (handler.handles(el)) {
                        log.info("--------------------------------------------------------------------------------");
                        log.info("Handling element '{}'", node.getNodeName());
                        handler.handle(el, st, st.env);
                        String input = el.getAttribute("input");
                        log.info("--------------------------------------------------------------------------------");
                        if (ProcessListHandler.class.isInstance(handler)) {
                            // apply processors
                            FlinkProcessList function = (FlinkProcessList) handler.getFunction();
                            if (!sources.containsKey(input)) {
                                log.error("Input '{}' has not been defined. Define 'stream' or " +
                                        "put process list after the process list defining the " +
                                        "output queue with this input name.", input);
                                return st;
                            }

                            //TODO: add parallelism
                            DataStream<Data> dataStream = sources.get(input)
                                    .flatMap(function)
                                    .setParallelism(4);

                            // detect output queues
                            List<String> outputQueues = function.getListOfOutputQueues();

                            // split the data stream if there are any queues used inside
                            // of process list
                            if (outputQueues.size() > 0) {
                                splitDataStream(sources, dataStream, outputQueues);
                            }
                        }
                    }
                }
            }
        }

        st.env.execute();
        return st;
    }

    /**
     * For a list of processors enqueueing items split the DataStream and put the selected new data
     * streams into the hashmap.
     *
     * @param sources      hash map containing data streams
     * @param dataStream   data stream used for split
     * @param outputQueues list of queues used for the output
     */
    private static void splitDataStream(HashMap<String, DataStream<Data>> sources,
                                        DataStream<Data> dataStream,
                                        List<String> outputQueues) {
        final List<String> allQueues = outputQueues;
        SplitStream<Data> split = dataStream.split(new OutputSelector<Data>() {
            @Override
            public Iterable<String> select(Data data) {
                List<String> queues = new ArrayList<>(allQueues.size());
                try {
                    if (data.containsKey("flink.queue")) {
                        String outputQueue = (String) data.get("flink.queue");
                        log.debug("flink.queue {}", outputQueue);
                        for (String queue : allQueues) {
                            if (queue.equals(outputQueue)) {
                                queues.add(queue);
                            }
                        }
                    }
                } catch (NullPointerException ex) {
                    log.error("Data item is empty.");
                }
                return queues;
            }
        });
        for (String queue : allQueues) {
            sources.put(queue, split.select(queue));
        }
    }
}


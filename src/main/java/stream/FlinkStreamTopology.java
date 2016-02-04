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

import flink.config.FlinkConfigHandler;
import flink.config.ProcessListHandler;
import flink.config.QueueHandler;
import flink.config.SourceHandler;
import flink.functions.FlinkProcessList;
import flink.functions.FlinkQueue;
import stream.runtime.setup.factory.ObjectFactory;
import stream.storm.Constants;
import stream.util.Variables;
import stream.util.XIncluder;
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
     * Creates a new instance of a StreamTopology based on the given document and using stream
     * execution environment
     *
     * @param doc The DOM document that defines the topology.
     */
    public static FlinkStreamTopology create(Document doc) throws Exception {

        final FlinkStreamTopology st = new FlinkStreamTopology();
        st.env = StreamExecutionEnvironment.getExecutionEnvironment();

        // add unique IDs
        doc = XMLUtils.addUUIDAttributes(doc, Constants.UUID_ATTRIBUTE);

        // search for 'application' or 'container' tag and extract its ID
        st.getVariables().put(Constants.APPLICATION_ID, getAppId(doc));

        // handle <include../>
        doc = new XIncluder().perform(doc, st.getVariables());

        // handle properties and save them to variables
        st.getVariables().addVariables(StreamTopology.handleProperties(doc, st.getVariables()));

        // create stream sources (multiple are possible)
        HashMap<String, DataStream<Data>> sources = initFlinkSources(doc, st);
        if (sources == null) {
            log.error("No source was or could have been initialized.");
            return null;
        }

        // create all possible queues
        initFlinkQueues(doc, st);

        // create processor list handler and apply it to ProcessorLists
        if (initFlinkFunctions(doc, st, sources)) {
            // set level of parallelism for the job
            st.env.setParallelism(getParallelism(doc.getDocumentElement()));
            // execute flink job if we were able to init all the functions
            st.env.execute(st.getVariables().get(Constants.APPLICATION_ID));
        }
        return st;
    }

    /**
     * Find ProcessorLists and handle them to become FlatMap functions.
     *
     * @param doc     XML document
     * @param st      stream topology
     * @param sources list of sources / queues
     * @return true if all functions can be applied; false if something goes wrong.
     */
    private static boolean initFlinkFunctions(Document doc, FlinkStreamTopology st, HashMap<String, DataStream<Data>> sources) {
        //TODO we do not use a list here, but only one handler at the moment
        ArrayList<FlinkConfigHandler> handlers = new ArrayList<>();
        handlers.add(new ProcessListHandler(ObjectFactory.newInstance()));
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
                        try {
                            handler.handle(el, st);
                        } catch (Exception e) {
                            log.error("Handler {} could not handle element {}.", handler, el);
                            return false;
                        }
                        String input = el.getAttribute("input");
                        log.info("--------------------------------------------------------------------------------");
                        if (ProcessListHandler.class.isInstance(handler)) {
                            // apply processors
                            FlinkProcessList function = (FlinkProcessList) handler.getFunction();
                            if (!sources.containsKey(input)) {
                                log.error("Input '{}' has not been defined. Define 'stream' or " +
                                        "put process list after the process list defining the " +
                                        "output queue with this input name.", input);
                                return false;
                            }

                            DataStream<Data> dataStream = sources.get(input)
                                    .flatMap(function)
                                    .setParallelism(getParallelism(el));

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
        return true;
    }

    /**
     * Inspect attributes of the given element whether they contain special attribute to define
     * level of parallelism. If nothing defined, return 1.
     *
     * @param element part of xml
     * @return level of parallelism defined in xml if attribute found; otherwise: 1
     */
    private static int getParallelism(Element element) {
        if (element.hasAttribute(Constants.NUM_WORKERS)) {
            return Integer.valueOf(element.getAttribute(Constants.NUM_WORKERS));
        }
        return 1;
    }

    /**
     * Search for application id in application and container tags. Otherwise produce random UUID.
     *
     * @param doc XML document
     * @return application id extracted from the ID attribute or random UUID if no ID attribute
     * present
     */
    private static String getAppId(Document doc) {
        String appId = "application:" + UUID.randomUUID().toString();

        // try to find application or container tags
        NodeList nodeList = doc.getElementsByTagName("application");
        if (nodeList.getLength() < 1) {
            nodeList = doc.getElementsByTagName("container");
        }

        // do there exist more than one application or container tags?
        if (nodeList.getLength() > 1) {
            log.error("More than 1 application node.");
        } else {
            appId = nodeList.item(0).getAttributes().getNamedItem("id").getNodeValue();
        }
        return appId;
    }

    /**
     * Find all sources (streams) and wrap them in FlinkSources.
     *
     * @param doc XML document
     * @param st  stream topology
     */
    private static HashMap<String, DataStream<Data>> initFlinkSources(Document doc, FlinkStreamTopology st) {
        NodeList streamList = doc.getDocumentElement().getElementsByTagName("stream");
        if (streamList.getLength() < 1) {
            log.debug("At least 1 stream source has to be defined.");
            return null;
        }

        ObjectFactory of = ObjectFactory.newInstance();
        HashMap<String, DataStream<Data>> sources = new HashMap<>(streamList.getLength());
        SourceHandler sourceHandler = new SourceHandler(of);
        //TODO use something more simple?!
        for (int is = 0; is < streamList.getLength(); is++) {
            Element item = (Element) streamList.item(is);
            if (sourceHandler.handles(item)) {
                // name of the source
                String id = item.getAttribute("id");

                // handle the source and create data stream for it
                try {
                    sourceHandler.handle(item, st);
                } catch (Exception e) {
                    log.error("Error while handling the source for item {}", item);
                    return null;
                }
                DataStream<Data> source = st.env
                        .addSource(sourceHandler.getFunction(), id)
                        .setParallelism(getParallelism(item));
//                        .disableChaining();

                // put this source into the hashmap
                sources.put(id, source);
                log.info("'{}' added as stream source.", id);
            } else {
                log.debug("Source handler doesn't handle {}", item.toString());
            }
        }
        return sources;
    }

    /**
     * Find all queues and wrap them in FlinkQueues.
     *
     * @param doc XML document
     * @param st  stream topology
     */
    private static void initFlinkQueues(Document doc, FlinkStreamTopology st) throws Exception {
        NodeList queueList = doc.getDocumentElement().getElementsByTagName("queue");
        ObjectFactory of = ObjectFactory.newInstance();
        QueueHandler queueHandler = new QueueHandler(of);
        for (int iq = 0; iq < queueList.getLength(); iq++) {
            Element element = (Element) queueList.item(iq);
            if (queueHandler.handles(element)) {
                queueHandler.handle(element, st);
                FlinkQueue flinkQueue = (FlinkQueue) queueHandler.getFunction();
                st.flinkQueues.add(flinkQueue);
            }
        }
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


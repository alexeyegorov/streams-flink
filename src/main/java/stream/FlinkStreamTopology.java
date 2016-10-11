package stream;

import org.apache.flink.streaming.api.TimeCharacteristic;
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

import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;

import javax.xml.transform.OutputKeys;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import flink.config.ProcessListHandler;
import flink.config.QueueHandler;
import flink.config.ServiceHandler;
import flink.config.SourceHandler;
import flink.functions.FlinkProcessList;
import flink.functions.FlinkQueue;
import flink.functions.FlinkService;
import stream.runtime.setup.factory.ObjectFactory;
import stream.util.Variables;
import stream.util.XIncluder;

/**
 * Topology builder similar to streams-storm builder.
 *
 * @author alexey
 */
public class FlinkStreamTopology {

    private static Logger log = LoggerFactory.getLogger(FlinkStreamTopology.class);

    public final Variables variables = new Variables();
    private Document doc;

    /**
     * List of queues used for inter-process communication.
     */
    public List<FlinkQueue> flinkQueues = new ArrayList<>(0);

    /**
     * List of services
     */
    public List<FlinkService> flinkServices = new ArrayList<>(0);

    /**
     * Stream execution environment created to execute Flink topology.
     */
    private StreamExecutionEnvironment env;

    public FlinkStreamTopology(Document doc) {
        this.doc = doc;
        this.env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
    }

    public Variables getVariables() {
        return variables;
    }

    /**
     * Creates a new instance of a StreamTopology based on the given document and using stream
     * execution environment
     */
    public boolean createTopology() throws Exception {
        // search for 'application' or 'container' tag and extract its ID
        variables.put(Constants.APPLICATION_ID, getAppId(doc));

        // handle <include.../>
        doc = new XIncluder().perform(doc, variables);

        //TODO remove output of XML or add it as a method
        TransformerFactory tf = TransformerFactory.newInstance();
        javax.xml.transform.Transformer transformer = tf.newTransformer();
        transformer.setOutputProperty(OutputKeys.OMIT_XML_DECLARATION, "no");
        transformer.setOutputProperty(OutputKeys.METHOD, "xml");
        transformer.setOutputProperty(OutputKeys.INDENT, "yes");
        transformer.setOutputProperty(OutputKeys.ENCODING, "UTF-8");
        transformer.setOutputProperty("{http://xml.apache.org/xslt}indent-amount", "4");

        transformer.transform(new DOMSource(doc),
                new StreamResult(new OutputStreamWriter(System.out, "UTF-8")));

        // handle properties and save them to variables
        variables.addVariables(StreamTopology.handleProperties(doc, variables));

        // handle <service.../>
        initFlinkServices(doc);

        // create stream sources (multiple are possible)
        HashMap<String, DataStream<Data>> sources = initFlinkSources(doc);
        if (sources == null) {
            log.error("No source was or could have been initialized.");
            return false;
        }

        // create all possible queues
        initFlinkQueues(doc);

        // create processor list handler and apply it to ProcessorLists
        return initFlinkFunctions(doc, sources);
    }

    /**
     * Execute previously created topology.
     */
    public void executeTopology() throws Exception {
        // set level of parallelism for the job
        env.setParallelism(getParallelism(doc.getDocumentElement()));
        // execute flink job if we were able to init all the functions
//        System.out.println(env.getExecutionPlan());
        env.execute(getVariables().get(Constants.APPLICATION_ID));
    }

    /**
     * Find all queues and wrap them in FlinkQueues.
     *
     * @param doc XML document
     */
    private void initFlinkServices(Document doc) throws Exception {
        NodeList serviceList = doc.getDocumentElement().getElementsByTagName("service");
        ServiceHandler serviceHandler = new ServiceHandler(ObjectFactory.newInstance());
        for (int iq = 0; iq < serviceList.getLength(); iq++) {
            Element element = (Element) serviceList.item(iq);
            if (serviceHandler.handles(element)) {
                serviceHandler.handle(element, this);
                FlinkService flinkService = (FlinkService) serviceHandler.getFunction();
                flinkServices.add(flinkService);
            }
        }
    }

    /**
     * Find all sources (streams) and wrap them in FlinkSources.
     *
     * @param doc XML document
     */
    private HashMap<String, DataStream<Data>> initFlinkSources(Document doc) {
        NodeList streamList = doc.getDocumentElement().getElementsByTagName("stream");
        if (streamList.getLength() < 1) {
            log.debug("At least 1 stream source has to be defined.");
            return null;
        }

        ObjectFactory of = ObjectFactory.newInstance();
        HashMap<String, DataStream<Data>> sources = new HashMap<>(streamList.getLength());
        SourceHandler sourceHandler = new SourceHandler(of);
        for (int is = 0; is < streamList.getLength(); is++) {
            Element item = (Element) streamList.item(is);
            if (sourceHandler.handles(item)) {
                // name of the source
                String id = item.getAttribute("id");

                // handle the source and create data stream for it
                try {
                    sourceHandler.handle(item, this);
                } catch (Exception e) {
                    log.error("Error while handling the source for item {}", item);
                    return null;
                }
                DataStream<Data> source = env
                        .addSource(sourceHandler.getFunction(), id)
                        .setParallelism(getParallelism(item))
                        .name(id);

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
     * Find ProcessorLists and handle them to become FlatMap functions.
     *
     * @param doc     XML document
     * @param sources list of sources / queues
     * @return true if all functions can be applied; false if something goes wrong.
     */
    private boolean initFlinkFunctions(Document doc, HashMap<String, DataStream<Data>> sources) {
        // check if any function is found to be applied onto data stream
        // flink topology won't stop, if some queue is mentioned but not used and if processor list
        // is using an input queue that is not filled
        boolean anyFunctionFound = false;

        // create processor list handler
        ProcessListHandler handler = new ProcessListHandler(ObjectFactory.newInstance());

        NodeList list = doc.getDocumentElement().getElementsByTagName("process");
        int length = list.getLength();
        for (int i = 0; i < length; i++) {
            Node node = list.item(i);
            if (node.getNodeType() == Node.ELEMENT_NODE) {
                final Element element = (Element) node;

                if (handler.handles(element)) {
                    log.info("--------------------------------------------------------------------------------");
                    log.info("Handling element '{}'", node.getNodeName());
                    try {
                        handler.handle(element, this);
                    } catch (Exception e) {
                        log.error("Handler {} could not handle element {}.", handler, element);
                        return false;
                    }

                    // distinguish whether the input is coming through the 'input' attribute
                    // or through the 'output' attribute of another processor
                    String input;
                    if (element.hasAttribute("input")){
                        input = element.getAttribute("input");
                    } else if (sources.containsKey(element.getAttribute("id"))){
                        input = element.getAttribute("id");
                    } else {
                        // stop processing as not input or output to this process has been found
                        log.error("It is not possible to find the stream to this process.");
                        return false;
                    }

                    log.info("--------------------------------------------------------------------------------");
                    if (ProcessListHandler.class.isInstance(handler)) {
                        // apply processors
                        FlinkProcessList function = (FlinkProcessList) handler.getFunction();
                        if (!sources.containsKey(input)) {
                            log.error("Input '{}' has not been defined or no other processor is " +
                                    "filling this input queue. Define 'stream' or " +
                                    "put processor list after the processor list defining the " +
                                    "output queue with this input name.", input);
                            continue;
                        }

                        DataStream<Data> dataStream = sources.get(input);

                        // group the stream by the given key
                        if (function.getGroupBy() != null && !function.getGroupBy().trim().equals("")){
                            dataStream = dataStream.keyBy(
                                    data -> (String) data.get(function.getGroupBy()));
                        }

                        // each data stream without any following queue can do a rescale
                        // meaning that previous exec. plan will be rescaled
    //                                .rescale()
                        // apply the processors
                        dataStream = dataStream
                                .flatMap(function)
                                // set the level of parallelism
                                .setParallelism(getParallelism(element))
                                // define the name for this process
                                .name(element.getAttribute("id"));

                        // detect output queues
                        List<String> outputQueues = function.getListOfOutputQueues();

                        boolean followingProcess = false;
                        // split the data stream if there are any queues used inside
                        // of process list
                        if (outputQueues.size() > 0) {
                            splitDataStream(sources, dataStream, outputQueues);
                            followingProcess = true;
                        }
                        // if this element has 'output' attribute,
                        // put the outcoming data stream into the list of the sources
                        if (element.hasAttribute("output")) {
                            String output = element.getAttribute("output");
                            if (output.trim().length() > 0) {
                                if (output.indexOf(",") > 0) {
                                    for (String out : output.split(",")) {
                                        sources.put(out, dataStream);
                                    }
                                } else {
                                    sources.put(output, dataStream);
                                }
                            }
                            followingProcess = true;
                        }
                        // if no further processors are used after this function,
                        // then do a rescale
                        if (!followingProcess) {
                            dataStream.rescale();
                        }
                        anyFunctionFound = true;
                    }
                }
            }
        }
        return anyFunctionFound;
    }

    /**
     * Find all queues and wrap them in FlinkQueues.
     *
     * @param doc XML document
     */
    private void initFlinkQueues(Document doc) throws Exception {
        NodeList queueList = doc.getDocumentElement().getElementsByTagName("queue");
        ObjectFactory of = ObjectFactory.newInstance();
        QueueHandler queueHandler = new QueueHandler(of);
        for (int iq = 0; iq < queueList.getLength(); iq++) {
            Element element = (Element) queueList.item(iq);
            if (queueHandler.handles(element)) {
                queueHandler.handle(element, this);
                FlinkQueue flinkQueue = (FlinkQueue) queueHandler.getFunction();
                flinkQueues.add(flinkQueue);
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
                    if (data.containsKey(Constants.FLINK_QUEUE)) {
                        String outputQueue = (String) data.get(Constants.FLINK_QUEUE);
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

    /**
     * Inspect attributes of the given element whether they contain special attribute to define
     * level of parallelism. If nothing defined, return 1.
     *
     * @param element part of xml
     * @return level of parallelism defined in xml if attribute found; otherwise: 1
     */
    private static int getParallelism(Element element) {
        if (element.hasAttribute(Constants.NUM_WORKERS)) {
            try {
                return Integer.valueOf(element.getAttribute(Constants.NUM_WORKERS));
            } catch (NumberFormatException ex) {
                log.error("Unable to parse defined level of parallelism: {}\n" +
                                "Returning default parallelism level: {}",
                        element.getAttribute(Constants.NUM_WORKERS), Constants.DEFAULT_PARALLELISM);
            }
        }
        return Constants.DEFAULT_PARALLELISM;
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
}


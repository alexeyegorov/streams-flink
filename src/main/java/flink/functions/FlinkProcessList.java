package flink.functions;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import flink.QueueInjection;
import flink.ServiceInjection;
import stream.Data;
import stream.FlinkStreamTopology;
import stream.ProcessContext;
import stream.Processor;
import stream.ProcessorList;
import stream.StatefulProcessor;
import stream.runtime.setup.factory.ObjectFactory;
import stream.runtime.setup.factory.ProcessorFactory;
import stream.storm.Constants;
import stream.util.Variables;

/**
 * Own implementation of FlatMapFunction for a list of processors (<process>...</process>). FlatMap
 * required to be sure all items stored in queues are collected.
 *
 * @author alexey
 */
public class FlinkProcessList extends StreamsFlinkObject implements FlatMapFunction<Data, Data> {

    static Logger log = LoggerFactory.getLogger(FlinkProcessList.class);


    //TODO: how does this work in a real cluster?!
    /**
     * Number of workers to be used for performance measuring of each worker
     */
    static int THREAD_NUMBER;

    /**
     * List of queues
     */
    private List<FlinkQueue> flinkQueues;

    /**
     * List of services
     */
    private List<FlinkService> flinkServices;

    /**
     * List of processors to be executed
     */
    protected ProcessorList process;

    /**
     * Variables with environment information
     */
    protected Variables variables;

    /**
     * Document element containing information about list of processors.
     */
    protected Element element;

    /**
     * Process context is used for initialization and is realized here by using FlinkContext.
     */
    protected ProcessContext context;

    public FlinkProcessList(FlinkStreamTopology streamTopology, Element el) {
        this.variables = streamTopology.getVariables();
        this.element = el;
        String processId;
        if (el.hasAttribute("id")) {
            processId = el.getAttribute("id");
        } else {
            processId = UUID.randomUUID().toString();
        }
        this.context = new FlinkContext(processId);
        this.context.set(Constants.APPLICATION_ID,
                streamTopology.variables.get(Constants.APPLICATION_ID));

        // add only queues that are used in this ProcessorList
        List<String> listOfOutputQueues = getListOfOutputQueues();
        flinkQueues = new ArrayList<>(0);
        for (FlinkQueue queue : streamTopology.flinkQueues) {
            if (listOfOutputQueues.contains(queue.getQueueName().toLowerCase())) {
                flinkQueues.add(queue);
            }
        }

        // add services
        this.flinkServices = streamTopology.flinkServices;

        log.debug("Processors for '" + el + "' initialized.");
    }

    @Override
    public void flatMap(Data data, Collector<Data> collector) throws Exception {
        if (data != null) {
            process.process(data);

            // go through all queues and collect written data items
            for (FlinkQueue q : flinkQueues) {
                while (q.getSize() > 0) {
                    collector.collect(q.read());
                }
            }
        }
    }

    @Override
    protected void init() throws Exception {
        String id = element.getAttribute("id") + "-node-" + THREAD_NUMBER++;
        element.setAttribute("id", id);
        context.set("process", id);
        process = createProcess();
        for (Processor p : process.getProcessors()) {
            if (p instanceof StatefulProcessor) {
                ((StatefulProcessor) p).init(context);
            }
        }
        log.info("Initializing ProcessorList {} with element.id {}", process, element.getAttribute("id"));
    }

    /**
     * This method creates the inner processors of this process bolt.
     *
     * @return list of processors inside a function
     */
    protected ProcessorList createProcess() throws Exception {
        ObjectFactory obf = ObjectFactory.newInstance();
        obf.addVariables(variables);
        ProcessorFactory pf = new ProcessorFactory(obf);

        // The handler injects wrappers for any QueueService accesses, thus
        // effectively doing the queue-flow injection
        //
        QueueInjection queueInjection = new QueueInjection(flinkQueues);
        pf.addCreationHandler(queueInjection);

        ServiceInjection serviceInjection = new ServiceInjection(flinkServices);
        pf.addCreationHandler(serviceInjection);

        log.debug("Creating processor-list from element {}", element);
        List<Processor> list = pf.createNestedProcessors(element);

        process = new ProcessorList();
        for (Processor p : list) {
            process.getProcessors().add(p);
        }
        return process;
    }

    /**
     * Go through the list of processors and check which queues are used as their output.
     *
     * @return list of queues as string
     */
    public List<String> getListOfOutputQueues() {
        return getOutputQueues(this.element);
    }

    /**
     * Go recursively through all children of each element and check if they have 'queue' or
     * 'queues' attribute.
     *
     * @param element part of XML configuration containing list of processors.
     * @return list of queues as string
     */
    private List<String> getOutputQueues(Element element) {
        List<String> output = new ArrayList<>(0);
        NodeList childNodes = element.getChildNodes();
        for (int el = 0; el < childNodes.getLength(); el++) {
            Node item = childNodes.item(el);
            if (item.getNodeType() == Node.ELEMENT_NODE) {
                Element child = (Element) item;
                if (child.hasAttribute("queue")) {
                    output.add(child.getAttribute("queue"));
                }
                if (child.hasAttribute("queues")) {
                    String queues = child.getAttribute("queues");
                    String[] split = queues.split(",");
                    for (String queue : split) {
                        output.add(queue.trim());
                    }
                }
                output.addAll(getOutputQueues(child));
            }
        }
        return output;
    }
}

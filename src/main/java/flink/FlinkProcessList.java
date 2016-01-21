package flink;

import org.apache.flink.api.common.functions.MapFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

import java.io.ByteArrayInputStream;
import java.util.List;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import stream.Data;
import stream.ProcessContext;
import stream.Processor;
import stream.ProcessorList;
import stream.StatefulProcessor;
import stream.StormRunner;
import stream.runtime.setup.factory.ObjectFactory;
import stream.runtime.setup.factory.ProcessorFactory;
import stream.util.Variables;

/**
 * @author alexey
 */
public class FlinkProcessList extends StreamsFlinkObject implements MapFunction<Data, Data> {

    static Logger log = LoggerFactory.getLogger(FlinkProcessList.class);

    protected ProcessorList process;
    protected Variables variables;
    protected Element element;
    protected ProcessContext context;

    public FlinkProcessList(ProcessorList process){
        this.process = process;
    }

    public FlinkProcessList(Variables variables, Element el) {
        this.variables = variables;
        this.element = el;
        this.context = new FlinkContext("");
        log.debug("Processors for '" + el + "' initialized.");
    }

    @Override
    public Data map(Data data) throws Exception {
        if (data != null){
            data = process.process(data);
        }
        return data;
    }

    @Override
    protected void init() throws Exception {
        process = createProcess();
        for(Processor p : process.getProcessors()){
            ((StatefulProcessor) p).init(context);
        }
    }

    /**
     * This method creates the inner processors of this function bolt.
     *
     * @return list of processors inside a function
     * @throws Exception
     */
//     * @param variables
//     * @param id
    protected ProcessorList createProcess() throws Exception {

//        DocumentBuilder builder = DocumentBuilderFactory.newInstance().newDocumentBuilder();
//        Document config = builder.parse(new ByteArrayInputStream(element.getBytes()));

//        Element element = StormRunner.findElementByUUID(config.getDocumentElement(), id);

//        if (element == null) {
//            log.error("Failed to find function for uuid '{}' in the XML!", id);
//            throw new Exception("Failed to find function for uuid '" + id + "' in the XML!");
//        }

        ObjectFactory obf = ObjectFactory.newInstance();
        obf.addVariables(variables);
        ProcessorFactory pf = new ProcessorFactory(obf);

        // The handler injects wrappers for any QueueService accesses, thus
        // effectively doing the queue-flow injection
        //
//        QueueInjection queueInjection = new QueueInjection(uuid, output);
//        pf.addCreationHandler(queueInjection);

        log.debug("Creating processor-list from element {}", element);
        List<Processor> list = pf.createNestedProcessors(element);

        process = new ProcessorList();
        for (Processor p : list) {
            process.getProcessors().add(p);
        }

//        if (element.hasAttribute("output")) {
//            String out = element.getAttribute("output");
//            if (out.indexOf(",") > 0) {
//                outputs = out.split(",");
//            } else {
//                outputs = new String[] { out };
//            }
//        }

//        subscriptions.addAll(queueInjection.getSubscriptions());
//        log.debug("Found {} subscribers for bolt '{}': " + subscriptions, subscriptions.size(), uuid);
        return process;
    }
}

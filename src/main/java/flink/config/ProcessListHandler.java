package flink.config;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Element;

import flink.FlinkProcessList;
import stream.StreamTopology;
import stream.runtime.setup.factory.ObjectFactory;
import stream.storm.Constants;

/**
 * @author alexey
 */
public class ProcessListHandler extends FlinkConfigHandler {

    static Logger log = LoggerFactory.getLogger(ProcessListHandler.class);
    transient FlinkProcessList function;

    protected String xml;

    public ProcessListHandler(ObjectFactory of, String xml) {
        this(of);
        this.xml = xml;
    }

    private ProcessListHandler(ObjectFactory of) {
        super(of);
    }

    public FlinkProcessList getFunction() {
        return function;
    }

    @Override
    public void handle(Element el, StreamTopology st, StreamExecutionEnvironment env) throws Exception {
        if (el.getNodeName().equalsIgnoreCase("process")) {
            String id = el.getAttribute(Constants.ID);
            if (id == null || id.trim().isEmpty()) {
                log.error("No 'id' attribute defined in process element (class: '{}')", el.getAttribute("class"));
                throw new Exception("Missing 'id' attribute for process element!");
            }

            log.info("  > Creating process-bolt with id '{}'", id);

            String copies = el.getAttribute("copies");
            Integer workers = 1;
            if (copies != null && !copies.isEmpty()) {
                try {
                    workers = Integer.parseInt(copies);
                } catch (Exception e) {
                    throw new RuntimeException("Invalid number of copies '" + copies + "' specified!");
                }
            }


            function = new FlinkProcessList(st.getVariables(), el);
            // createProcess(st.getVariables(), id);

//            ProcessBolt bolt = new ProcessBolt(xml, id, st.getVariables());
//            log.info("  >   Registering bolt (function) '{}' with instance {}", id, bolt);

//            List<String> inputs = getInputNames(el);
////            BoltDeclarer cur = builder.setBolt(id, bolt, workers);
//            if (!inputs.isEmpty()) {
//                for (String in : inputs) {
//                    if (!in.isEmpty()) {
//                        //
//                        // if 'in' is reference to a function/bolt
//                        //
//
//                        //
//                        // else
//                        //
//                        log.info("  >   Connecting bolt '{}' to non-group '{}'", id, in);
//                        st.addSubscription(new Subscription(id, in));
//                        // cur = cur.noneGrouping(in);
//                    }
//                }
//            } else {
//                log.warn("No input defined for function '{}'!", id);
//            }
//            st.addBolt(id, cur);
//
//            for (Subscription subscription : bolt.getSubscriptions()) {
//                log.info("Adding subscription:  {}", subscription);
//                st.addSubscription(subscription);
//            }
        }
    }


    @Override
    public boolean handles(Element el) {
        String name = el.getNodeName();
        return name.equalsIgnoreCase("process");
    }
}
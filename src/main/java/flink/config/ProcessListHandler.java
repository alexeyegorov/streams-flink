package flink.config;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Element;

import flink.FlinkProcessList;
import stream.FlinkStreamTopology;
import stream.runtime.setup.factory.ObjectFactory;
import stream.storm.Constants;

/**
 * Configuration handler for list of processors. Method handle(...) creates FlatMapFunction to
 * process the processors that could enqueue items into queues.
 *
 * @author alexey
 */
public class ProcessListHandler extends FlinkConfigHandler {

    static Logger log = LoggerFactory.getLogger(ProcessListHandler.class);

    protected String xml;

    public ProcessListHandler(ObjectFactory of, String xml) {
        super(of);
        this.xml = xml;
    }

    @Override
    public void handle(Element el, FlinkStreamTopology st, StreamExecutionEnvironment env) throws Exception {
        if (handles(el)) {
            String id = el.getAttribute(Constants.ID);
            if (id == null || id.trim().isEmpty()) {
                log.error("No 'id' attribute defined in process element (class: '{}')", el.getAttribute("class"));
                throw new Exception("Missing 'id' attribute for process element!");
            }

            log.info("  > Creating process-bolt with id '{}'", id);

            //TODO: add parallelism
            String copies = el.getAttribute("copies");
            Integer workers = 1;
            if (copies != null && !copies.isEmpty()) {
                try {
                    workers = Integer.parseInt(copies);
                } catch (Exception e) {
                    throw new RuntimeException("Invalid number of copies '" + copies + "' specified!");
                }
            }

            function = new FlinkProcessList(st, el);
        }
    }

    @Override
    public boolean handles(Element el) {
        return handles(el, "process");
    }
}

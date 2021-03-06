package flink.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Element;

import flink.functions.FlinkProcessList;
import stream.FlinkStreamTopology;
import stream.runtime.setup.factory.ObjectFactory;
import stream.Constants;

/**
 * Configuration handler for list of processors. Method handle(...) creates FlatMapFunction to
 * process the processors that could enqueue items into queues.
 *
 * @author alexey
 */
public class ProcessListHandler extends FlinkConfigHandler {

    static Logger log = LoggerFactory.getLogger(ProcessListHandler.class);

    public ProcessListHandler(ObjectFactory objectFactory) {
        super(objectFactory);
    }

    @Override
    public void handle(Element el, FlinkStreamTopology st) throws Exception {
        if (handles(el)) {
            String id = el.getAttribute(Constants.ID);
            if (id == null || id.trim().isEmpty()) {
                log.error("No 'id' attribute defined in process element (class: '{}')", el.getAttribute("class"));
                throw new Exception("Missing 'id' attribute for process element!");
            }

            log.info("  > Creating process-function with id '{}'", id);

            function = new FlinkProcessList(st, el);
        }
    }

    @Override
    public boolean handles(Element el) {
        return handles(el, "process");
    }
}

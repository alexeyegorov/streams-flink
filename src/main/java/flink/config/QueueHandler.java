package flink.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Element;

import flink.functions.FlinkQueue;
import stream.FlinkStreamTopology;
import stream.runtime.setup.factory.ObjectFactory;

/**
 * Configuration handler for queues. Method handle(...) creates FlinkQueue which is not a flink
 * native function, but implements Function and Queue classes. It is used as a wrapper for a queue
 * class with extra functionality to add label 'flink.queue' to data items.
 *
 * @author alexey
 */
public class QueueHandler extends FlinkConfigHandler {

    static Logger log = LoggerFactory.getLogger(QueueHandler.class);

    public QueueHandler(ObjectFactory of) {
        super(of);
    }

    @Override
    public void handle(Element el, FlinkStreamTopology st) throws Exception {
        if (handles(el)) {
            String id = el.getAttribute("id");
            if (id == null || id.trim().isEmpty()) {
                log.error("No 'id' attribute defined in queue element: {}", el.toString());
                throw new Exception("Missing 'id' attribute for queue element!");
            }

            //TODO handle parallelism?

            function = new FlinkQueue(st, el);
        }
    }

    @Override
    public boolean handles(Element el) {
        return handles(el, "queue");
    }
}

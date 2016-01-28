package flink.config;

import org.apache.flink.api.common.functions.Function;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Element;

import flink.FlinkQueue;
import stream.StreamTopology;
import stream.runtime.setup.factory.ObjectFactory;

/**
 * @author alexey
 */
public class QueueHandler extends FlinkConfigHandler {

    static Logger log = LoggerFactory.getLogger(QueueHandler.class);

    private FlinkQueue function;

    public QueueHandler(ObjectFactory of) {
        super(of);
    }

    @Override
    public void handle(Element el, StreamTopology st, StreamExecutionEnvironment env) throws Exception {
        if(el.getNodeName().equals("queue")){
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
        return "queue".equals(el.getNodeName().toLowerCase());
    }

    @Override
    public Function getFunction() {
        return function;
    }
}

package flink;

import org.apache.flink.api.common.functions.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Element;

import java.util.Collection;

import stream.Data;
import stream.StreamTopology;
import stream.io.Queue;
import stream.runtime.setup.factory.ObjectFactory;
import stream.util.Variables;

/**
 * Wrapper around the real queue implementation.
 *
 * @author alexey
 */
public class FlinkQueue extends StreamsFlinkObject implements Function, Queue {

    static Logger log = LoggerFactory.getLogger(FlinkQueue.class);

    private final Element element;
    private final String id;
    private final Variables variables;
    private Queue queue;

    public FlinkQueue(StreamTopology streamTopology, Element element) {
        this.element = element;
        variables = streamTopology.getVariables();
        queue = null;
        id = element.getAttribute("id");
    }

    @Override
    public void init() throws Exception {
        ObjectFactory obf = ObjectFactory.newInstance();
        obf.addVariables(variables);
        try {
            this.queue = (Queue) obf.create(element);
        } catch (ClassCastException ex) {
            log.debug("Queue seems not to be a right queue: {} with class {}",
                    element, obf.findClassForElement(element));
        }

    }

    @Override
    public String getId() {
        return queue.getId();
    }

    @Override
    public void setId(String id) {
        queue.setId(id);
    }

    @Override
    public Data read() throws Exception {
        return queue.read();
    }

    @Override
    public boolean write(Data item) throws Exception {
        item.put("flink.queue", id);
        return queue.write(item);
    }

    @Override
    public boolean write(Collection<Data> data) throws Exception {
        for(Data item : data){
            item.put("flink.queue", id);
        }
        return queue.write(data);
    }

    @Override
    public void close() throws Exception {
        queue.close();
    }

    @Override
    public void setCapacity(Integer limit) {
        queue.setCapacity(limit);
    }

    @Override
    public Integer getSize() {
        return queue.getSize();
    }

    @Override
    public Integer getCapacity() {
        return queue.getCapacity();
    }

    @Override
    public int clear() {
        return queue.clear();
    }
}

package flink.functions;

import org.apache.flink.api.common.functions.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Element;

import java.util.Collection;

import stream.Data;
import stream.FlinkStreamTopology;
import stream.io.Queue;
import stream.runtime.setup.factory.ObjectFactory;
import stream.util.Variables;

/**
 * While FlinkQueue implements Function and Queue classes, it is used as a wrapper for a queue class
 * with extra functionality to add label 'flink.queue' to data items.
 *
 * @author alexey
 */
public class FlinkQueue extends StreamsFlinkObject implements Function, Queue {

    static Logger log = LoggerFactory.getLogger(FlinkQueue.class);

    /**
     * Document element containing information about the queue
     */
    private Element element;

    /**
     * Name of the queue
     */
    private String id;

    /**
     * Variables with environment information
     */
    private Variables variables;

    /**
     * Real queue implementation
     */
    private Queue queue;

    public FlinkQueue(FlinkStreamTopology streamTopology, Element element) {
        this.element = element;
        variables = streamTopology.getVariables();
        queue = null;
        id = element.getAttribute("id");
    }

    /**
     * Retrieve the name of the queue
     *
     * @return queue name
     */
    public String getQueueName() {
        return id;
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
        //TODO: move to constants
        item.put("flink.queue", id);
        return queue.write(item);
    }

    @Override
    public boolean write(Collection<Data> data) throws Exception {
        for (Data item : data) {
            //TODO: move to constants
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

package flink;

import org.apache.flink.api.common.functions.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

import java.util.Collection;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

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

    private Element element;
    private String id;
    private Variables variables;
    private Queue queue;
    private boolean appended;

    public FlinkQueue(StreamTopology streamTopology, Element element) {
        this.element = element;
        variables = streamTopology.getVariables();
        queue = null;
        id = element.getAttribute("id");
        appended = false;
    }

    public FlinkQueue(StreamTopology streamTopology, String id) {
        DocumentBuilder documentBuilder;
        try {
            documentBuilder = DocumentBuilderFactory.newInstance().newDocumentBuilder();
        } catch (ParserConfigurationException e) {
            log.debug("Not able to create document builder to create an element for queue building.");
            return;
        }
        this.element = documentBuilder.newDocument().createElement("queue");
        element.setAttribute("id", id);
        element.setAttribute("class", "stream.io.BlockingQueue");
        this.id = id;
        this.variables = streamTopology.getVariables();
        appended = false;
    }

    public String getQueueName(){
        return id;
    }

    public boolean isAppended(){
        return appended;
    }

    public void setAppended(boolean appended){
        this.appended = appended;
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
        if (this.getSize()>1) {
            log.error("MORE THAN ONE ELEMENT in the queue");
        }
        return queue.read();
    }

    @Override
    public boolean write(Data item) throws Exception {
        item.put("flink.queue", id);
        appended = true;
        return queue.write(item);
    }

    @Override
    public boolean write(Collection<Data> data) throws Exception {
        for(Data item : data){
            item.put("flink.queue", id);
        }
        appended = false;
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

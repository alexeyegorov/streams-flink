package flink.functions;

import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Element;

import java.io.IOException;
import java.util.Map;

import stream.Data;
import stream.io.AbstractStream;
import stream.runtime.setup.factory.ObjectFactory;
import stream.util.Variables;

/**
 * Own source implementation to embed stream processor from 'streams framework'
 *
 * @author alexey
 */
public class FlinkSource extends StreamsFlinkObject implements ParallelSourceFunction<Data> {

    static Logger log = LoggerFactory.getLogger(FlinkSource.class);

    /**
     * Stream processor embedded inside of SourceFunction
     */
    protected static AbstractStream streamProcessor;

    /**
     * Flag to stop retrieving elements from the source.
     */
    private boolean isRunning = true;

    /**
     * Variables with environment information
     */
    protected Variables variables;

    /**
     * Element object containing part of XML file with configuration for the source.
     */
    private Element el;

    /**
     * Create new flink source object while saving XML's element with source configuration.
     *
     * @param element part of XML with source configuration
     */
    public FlinkSource(Variables variables, Element element) {
        this.variables = variables;
        this.el = element;
        log.debug("Source for '" + el + "' initialized.");
    }

    /**
     * init() is called inside of super class' readResolve() method.
     */
    protected synchronized void init() throws Exception {
        //TODO: does singleton works on distributed machine?
        if (getInstance() == null) {
            String className = el.getAttribute("class");
            ObjectFactory objectFactory = ObjectFactory.newInstance();
            Map<String, String> params = objectFactory.getAttributes(el);
            streamProcessor = (AbstractStream) objectFactory.create(
                    className, params, ObjectFactory.createConfigDocument(el), this.variables);
            streamProcessor.init();
        }
    }

    private static AbstractStream getInstance(){
        return streamProcessor;
    }

    @Override
    public void run(SourceContext<Data> ctx) throws Exception {
        if (streamProcessor == null) {
            log.debug("Stream processor has not been initialized properly.");
            return;
        }
        isRunning = true;
        while (isRunning) {
            // Stream processor retrieves next element by calling readNext() method
            // stop if stream is finished and produces NULL
            try {
                Data data = streamProcessor.read();
                if (data != null) {
                    ctx.collect(data);
                } else {
                    isRunning = false;
                }
            } catch (IOException exc){
                if (exc.getMessage().trim().toLowerCase().equals("stream closed")){
                    isRunning = false;
                }
            }
        }
    }

    @Override
    public void cancel() {
        log.debug("Cancelling FlinkSource '" + el + "'.");
        isRunning = false;
    }
}

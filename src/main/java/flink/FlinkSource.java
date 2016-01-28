package flink;

import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Element;

import java.util.Map;

import stream.Data;
import stream.io.AbstractStream;
import stream.runtime.setup.factory.ObjectFactory;

/**
 * Own source implementation to embed stream processor from 'streams framework'
 *
 * @author alexey
 */
public class FlinkSource extends StreamsFlinkObject implements SourceFunction<Data> {

    static Logger log = LoggerFactory.getLogger(FlinkSource.class);

    /**
     * Stream processor embedded inside of SourceFunction
     */
    protected AbstractStream streamProcessor;

    /**
     * Flag to stop retrieving elements from the source.
     */
    private boolean isRunning = true;

    /**
     * Element object containing part of XML file with configuration for the source.
     */
    private Element el;

    /**
     * Create new flink source object while saving XML's element with source configuration.
     *
     * @param element part of XML with source configuration
     */
    public FlinkSource(Element element) {
        this.el = element;
        log.debug("Source for '" + el + "' initialized.");
    }

    /**
     * init() is called inside of super class' readResolve() method.
     */
    protected void init() throws Exception {
        String className = el.getAttribute("class");
        ObjectFactory objectFactory = ObjectFactory.newInstance();
        Map<String, String> params = objectFactory.getAttributes(el);
        streamProcessor = (AbstractStream)
                objectFactory.create(className, params, ObjectFactory.createConfigDocument(el));
        streamProcessor.init();
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
            Data data = streamProcessor.readNext();
            if (data != null) {
                ctx.collect(data);
            } else {
                isRunning = false;
            }
        }
    }

    @Override
    public void cancel() {
        log.debug("Cancelling FlinkSource '" + el + "'.");
        isRunning = false;
    }


}

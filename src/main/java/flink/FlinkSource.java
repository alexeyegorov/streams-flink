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
 * @author alexey
 */
public class FlinkSource extends StreamsFlinkObject implements SourceFunction<Data> {

    static Logger log = LoggerFactory.getLogger(FlinkSource.class);

    protected AbstractStream streamProcessor;

    private boolean isRunning = true;

    private Element el;

    public FlinkSource(Element element){
        this.el = element;
        log.debug("Source for '" + el+ "' initialized.");
    }

    public FlinkSource(AbstractStream stream){
        this.streamProcessor = stream;
    }

    @Override
    public void run(SourceContext<Data> ctx) throws Exception {
        isRunning = true;
        while (isRunning) {
            ctx.collect(streamProcessor.readNext());
        }
    }

    protected void init() throws Exception {
        String className = el.getAttribute("class");
        ObjectFactory objectFactory = ObjectFactory.newInstance();
        Map<String, String> params = objectFactory.getAttributes(el);
        this.streamProcessor = (AbstractStream) objectFactory.create(
                className, params, ObjectFactory.createConfigDocument(el));
    }

    @Override
    public void cancel() {
        log.debug("Cancelling FlinkSource '" + el + "'.");
        isRunning = false;
    }
}

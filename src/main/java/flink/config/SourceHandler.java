package flink.config;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Element;

import java.util.Map;

import flink.FlinkSource;
import stream.Data;
import stream.FlinkStreamTopology;
import stream.runtime.setup.factory.ObjectFactory;
import stream.storm.Constants;

/**
 * @author alexey
 */
public class SourceHandler extends FlinkConfigHandler {

    static Logger log = LoggerFactory.getLogger(SourceHandler.class);

    protected SourceFunction<Data> function;

    public SourceHandler(ObjectFactory of) {
        super(of);
    }

    @Override
    public void handle(Element el, FlinkStreamTopology st, StreamExecutionEnvironment env)
            throws Exception {
        if (!handles(el)){
            return;
        }

        String id = el.getAttribute(Constants.ID);
        if (id == null) {
            throw new Exception("Element '" + el.getNodeName() + "' is missing an 'id' attribute!");
        }

        String className = el.getAttribute("class");
        Map<String, String> params = objectFactory.getAttributes(el);

        log.info("  > Found '{}' definition, with class: {}", el.getNodeName(), className);
        log.info("  >   Parameters are: {}", params);

        params = st.getVariables().expandAll(params);
        log.info("  >   Expanded parameters: {}", params);

        log.info("  >   Creating spout-instance from class {}, parameters: {}", className, params);
        function = new FlinkSource(el);
    }

    public SourceFunction<Data> getFunction(){
        return function;
    }

    @Override
    public boolean handles(Element el) {
        if (el == null) {
            return false;
        }
        String name = el.getNodeName();
        return "stream".equalsIgnoreCase(name);
    }
}

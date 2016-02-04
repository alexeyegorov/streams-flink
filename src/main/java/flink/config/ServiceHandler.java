package flink.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Element;

import java.util.Map;

import flink.functions.FlinkService;
import stream.FlinkStreamTopology;
import stream.runtime.setup.factory.ObjectFactory;

/**
 * Configuration handler for streams' services. Method handle(...) creates FlinkService to wrap the
 * underlying service.
 *
 * @author alexey
 */
public class ServiceHandler extends FlinkConfigHandler {

    static Logger log = LoggerFactory.getLogger(ServiceHandler.class);

    public ServiceHandler(ObjectFactory of) {
        super(of);
    }

    @Override
    public void handle(Element element, FlinkStreamTopology st) throws Exception {
        log.debug("handling element {}...", element);
        Map<String, String> params = objectFactory.getAttributes(element);

        String className = params.get("class");
        if (className == null || "".equals(className.trim())) {
            throw new Exception("No class name provided in 'class' attribute if Service element!");
        }

        String id = params.get("id");
        if (id == null || "".equals(id.trim())) {
            throw new Exception("No valid 'id' attribute provided for Service element!");
        }

        function = new FlinkService(st.getVariables(), element);
    }

    @Override
    public boolean handles(Element el) {
        return handles(el, "service");
    }
}

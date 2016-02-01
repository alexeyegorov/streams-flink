package flink.config;

import org.apache.flink.api.common.functions.Function;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.w3c.dom.Element;

import stream.FlinkStreamTopology;
import stream.runtime.setup.factory.ObjectFactory;

/**
 * @author alexey
 */
public abstract class FlinkConfigHandler {

    protected final ObjectFactory objectFactory;

    public FlinkConfigHandler(ObjectFactory of) {
        this.objectFactory = of;
    }

    /**
     * Override this method.
     * @param el
     * @param st
     * @param env
     */
    public abstract void handle(Element el, FlinkStreamTopology st, StreamExecutionEnvironment env) throws Exception;

    /**
     * Override this method.
     * @param el
     * @return
     */
    public abstract boolean handles(Element el);

    public abstract Function getFunction();
}

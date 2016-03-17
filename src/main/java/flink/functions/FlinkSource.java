package flink.functions;

import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

import java.io.IOException;
import java.util.Map;

import stream.Data;
import stream.io.AbstractStream;
import stream.io.multi.AbstractMultiStream;
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
    protected AbstractStream streamProcessor;

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
        String className = el.getAttribute("class");
        ObjectFactory objectFactory = ObjectFactory.newInstance();
        Map<String, String> params = objectFactory.getAttributes(el);
        streamProcessor = (AbstractStream) objectFactory.create(
                className, params, ObjectFactory.createConfigDocument(el), this.variables);
        if (el.hasChildNodes()) {
            if (streamProcessor instanceof AbstractMultiStream) {
                NodeList stream = el.getElementsByTagName("stream");
                for (int i = 0; i < stream.getLength(); i++) {
                    Element streamElement = (Element) stream.item(i);
                    AbstractStream subStream = (AbstractStream) objectFactory.create(
                            streamElement.getAttribute("class"), objectFactory.getAttributes(streamElement),
                            ObjectFactory.createConfigDocument(streamElement), this.variables);
                    ((AbstractMultiStream) streamProcessor)
                            .addStream(streamElement.getAttribute("id"), subStream);
                }
            }
        }
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
            try {
                Data data = streamProcessor.read();
                if (data != null) {
                    ctx.collect(data);
                } else {
                    isRunning = false;
                }
            } catch (IOException exc) {
                if (exc.getMessage().trim().toLowerCase().equals("stream closed")) {
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

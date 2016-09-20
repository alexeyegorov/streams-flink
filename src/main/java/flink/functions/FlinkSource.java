package flink.functions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Element;

import java.io.IOException;

import stream.Data;
import stream.DistributedStream;
import stream.io.Stream;
import stream.runtime.setup.factory.ObjectFactory;
import stream.runtime.setup.factory.StreamFactory;
import stream.util.Variables;

/**
 * Own source implementation to embed stream processor from 'streams framework'
 *
 * @author alexey
 */
public class FlinkSource extends StreamsFlinkSourceObject {

    private static Logger log = LoggerFactory.getLogger(FlinkSource.class);

    /**
     * Stream processor embedded inside of SourceFunction
     */
    private Stream streamProcessor;

    /**
     * Flag to stop retrieving elements from the source.
     */
    private boolean isRunning = true;

    /**
     * Variables with environment information
     */
    private Variables variables;

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
    public void init() throws Exception {
        streamProcessor = StreamFactory.createStream(ObjectFactory.newInstance(), el, variables);
        streamProcessor.init();
    }

    @Override
    public void run(SourceContext<Data> ctx) throws Exception {
        // if this is a distributed stream, then handle the parallelism level
        if (DistributedStream.class.isInstance(streamProcessor)) {
            DistributedStream parallelMultiStream = (DistributedStream) streamProcessor;
            try {
                Class<?> aClass = parallelMultiStream.getClass();
                aClass.getMethod("handleParallelism", int.class, int.class);

                // retrieve number of tasks and number of this certain task from the context
                int indexOfThisSubtask = getRuntimeContext().getIndexOfThisSubtask();
                int numberOfParallelSubtasks = getRuntimeContext().getNumberOfParallelSubtasks();

                // call the method to handle the parallelism level and then re-initialize the stream
                parallelMultiStream.handleParallelism(indexOfThisSubtask, numberOfParallelSubtasks);
                streamProcessor = parallelMultiStream;
                streamProcessor.init();
                log.info("Perform streaming in parallel mode ({}/{}).",
                        indexOfThisSubtask + 1, numberOfParallelSubtasks);
            } catch (NoSuchMethodException exc) {
                log.info("Stream is not prepared to be handled in parallel.");
            }
        }

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

package flink.functions;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Element;

import java.io.IOException;
import java.net.InetAddress;
import java.util.UUID;

import stream.Data;
import stream.FlinkStreamTopology;
import stream.Processor;
import stream.StatefulProcessor;

/**
 * Own implementation of FlatMapFunction for a list of processors (<process>...</process>). FlatMap
 * required to be sure all items stored in queues are collected.
 *
 * @author alexey
 */
public class FlinkStatefulProcessList extends FlinkProcessList {

    private static Logger log = LoggerFactory.getLogger(FlinkStatefulProcessList.class);

    public FlinkStatefulProcessList(FlinkStreamTopology streamTopology, Element el) {
        super(streamTopology, el);
        log.debug("Processors for '" + el + "' initialized.");
    }

    private transient ValueState<FlinkContext> state;

    @Override
    public void init() throws Exception {
        super.init();

        //TODO: move this implementation outside this class
        state = new ValueState<FlinkContext>() {
            FlinkContext context;

            @Override
            public FlinkContext value() throws IOException {
                return context;
            }

            @Override
            public void update(FlinkContext flinkContext) throws IOException {
                context = flinkContext;
                // TODO: update the single values!?
                // IDEA: while setting the values to the flinkContext, save the list of changes
                // and than update it
            }

            @Override
            public void clear() {
                context = null;
            }
        };

        log.info("Initializing ProcessorList {} with element.id {}", process, element.getAttribute("id"));
    }

    @Override
    public void flatMap(Data data, Collector<Data> collector) throws Exception {
        if (data != null) {
            context = state.value();
            super.flatMap(data, collector);
            state.update((FlinkContext) context);
        }
    }
}

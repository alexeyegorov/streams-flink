/*
 *  streams library
 *
 *  Copyright (C) 2011-2014 by Christian Bockermann, Hendrik Blom
 * 
 *  streams is a library, API and runtime environment for processing high
 *  volume data streams. It is composed of three submodules "stream-api",
 *  "stream-core" and "stream-runtime".
 *
 *  The streams library (and its submodules) is free software: you can 
 *  redistribute it and/or modify it under the terms of the 
 *  GNU Affero General Public License as published by the Free Software 
 *  Foundation, either version 3 of the License, or (at your option) any 
 *  later version.
 *
 *  The stream.ai library (and its submodules) is distributed in the hope
 *  that it will be useful, but WITHOUT ANY WARRANTY; without even the implied 
 *  warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU Affero General Public License for more details.
 *
 *  You should have received a copy of the GNU Affero General Public License
 *  along with this program.  If not, see http://www.gnu.org/licenses/.
 */
package flink;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Element;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import flink.functions.FlinkQueue;
import stream.Processor;
import stream.io.Sink;
import stream.runtime.DependencyInjection;
import stream.runtime.setup.factory.ObjectFactory;
import stream.runtime.setup.factory.ProcessorFactory.ProcessorCreationHandler;

/**
 * Original QueueInjection used in streams-storm project adapted for use with FlinkQueues
 *
 * @author alexey, christian
 */
public class QueueInjection implements ProcessorCreationHandler {

    static Logger log = LoggerFactory.getLogger(QueueInjection.class);

    /**
     * List of FlinkQueues used as wrapper for real queue implementations.
     */
    private final List<FlinkQueue> flinkQueues;

    public QueueInjection(List<FlinkQueue> flinkQueues) {
        this.flinkQueues = flinkQueues;
    }

    public static String getQueueSetterName(Method m) {
        return m.getName().substring(3);
    }

    /**
     * @see ProcessorCreationHandler#processorCreated(stream.Processor, Element)
     */
    @Override
    public void processorCreated(Processor processor, Element from) throws Exception {
        Map<String, String> params = ObjectFactory.newInstance().getAttributes(from);

        // iterate through all methods to find setter methods for (sub)class of Sink
        for (Method method : processor.getClass().getMethods()) {
            log.trace("Checking method {}", method);
            if (DependencyInjection.isSetter(method, Sink.class)) {
                final String qsn = getQueueSetterName(method);

                //TODO: is it necessary or would simply qsn.toLowerCase() would do the same?
                String prop = qsn.substring(0, 1).toLowerCase() + qsn.substring(1);

                if (params.get(prop) == null) {
                    log.debug("Found null-value for property '{}', " +
                            "skipping injection for this property.", prop);
                    continue;
                }

                log.debug("Found queue-setter for property {} (property value: '{}')",
                        prop, params.get(prop));

                if (DependencyInjection.isArraySetter(method, Sink.class)) {
                    // setter using array of comma separated queue names
                    String[] names = params.get(prop).split(",");

                    List<FlinkQueue> wrapper = new ArrayList<>();
                    for (String name : names) {
                        if (!name.trim().isEmpty()) {
                            FlinkQueue flinkQueue = getFlinkQueue(name);
                            if (flinkQueue != null) {
                                wrapper.add(flinkQueue);
                            } else {
                                log.debug("Queue '{}' was not found in the list of defined queues",
                                        name);
                            }
                        }
                    }
                    log.info("Injecting array of queues...");
                    Object array = wrapper.toArray(new FlinkQueue[wrapper.size()]);
                    method.invoke(processor, array);

                } else {
                    // setter using queue name
                    String name = params.get(prop);
                    log.info("Injecting a single queue... using method {}", method);
                    FlinkQueue flinkQueue = getFlinkQueue(name);
                    if (flinkQueue != null) {
                        method.invoke(processor, flinkQueue);
                    } else {
                        log.debug("Queue '{}' was not found in the list of defined queues", name);
                    }
                }
            } else {
                log.debug("Skipping method {} => not a queue-setter", method);
            }
        }
    }

    /**
     * Iterate through the list of all queues and find a queue with the given name
     *
     * @param name queue name
     * @return FlinkQueue
     */
    private FlinkQueue getFlinkQueue(String name) {
        for (FlinkQueue queue : flinkQueues) {
            if (queue.getQueueName().equals(name)) {
                return queue;
            }
        }
        return null;
    }
}
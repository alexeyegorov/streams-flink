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
package stream.storm;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Element;

import backtype.storm.task.OutputCollector;
import stream.Processor;
import stream.ProcessorList;
import stream.Subscription;
import stream.io.Sink;
import stream.runtime.DependencyInjection;
import stream.runtime.setup.factory.ObjectFactory;
import stream.runtime.setup.factory.ProcessorFactory.ProcessorCreationHandler;

/**
 * @author chris
 * 
 */
public class QueueInjection implements ProcessorCreationHandler {

	static Logger log = LoggerFactory.getLogger(QueueInjection.class);

	final OutputCollector collector;
	final String boltId;
	final Set<Subscription> subscriptions = new LinkedHashSet<Subscription>();

	public QueueInjection(String boltId, OutputCollector c) {
		this.boltId = boltId;
		this.collector = c;
	}

	public static void injectQueues(Processor proc, OutputCollector collector) {
		return;
	}

	public static void injectQueues(ProcessorList procs, OutputCollector collector) {
		for (Processor p : procs.getProcessors()) {
			injectQueues(p, collector);
		}
	}

	public static String getQueueSetterName(Method m) {
		return m.getName().substring(3);
	}

	public static boolean isQueueArraySetter(Method m) {
		Class<?> type = m.getParameterTypes()[0];
		return type.isArray();
	}

	/**
	 * @see stream.runtime.setup.factory.ProcessorFactory.ProcessorCreationHandler#processorCreated(stream.Processor,
	 *      org.w3c.dom.Element)
	 */
	@Override
	public void processorCreated(Processor p, Element from) throws Exception {
		Map<String, String> params = ObjectFactory.newInstance().getAttributes(from);
		for (Method m : p.getClass().getMethods()) {
			log.trace("Checking method {}", m);
			if (DependencyInjection.isSetter(m, Sink.class)) {
				final String qsn = getQueueSetterName(m);
				String prop = qsn.substring(0, 1).toLowerCase() + qsn.substring(1);

				if (params.get(prop) == null) {
					log.debug("Found null-value for property '{}', skipping injection for this property.", prop);
					continue;
				}

				log.debug("Found queue-setter for property {} (property value: '{}')", prop, params.get(prop));

				if (DependencyInjection.isArraySetter(m, Sink.class)) {
					String[] names = params.get(prop).split(",");

					List<QueueWrapper> wrapper = new ArrayList<QueueWrapper>();
					for (String name : names) {
						if (!name.trim().isEmpty()) {
							subscriptions.add(new Subscription(name.trim(), this.boltId, name));
							wrapper.add(new QueueWrapper(collector, name));
						}
					}
					log.debug("Injecting array of queues...");
					Object array = wrapper.toArray(new QueueWrapper[wrapper.size()]);
					m.invoke(p, array);

				} else {
					String name = params.get(prop);
					subscriptions.add(new Subscription(name.trim(), this.boltId, name));
					log.debug("Injecting a single queue... using method {}", m);
					m.invoke(p, new QueueWrapper(collector, name));
				}
			} else {
				log.debug("Skipping method {} => not a queue-setter", m);
			}
		}
	}

	public Set<Subscription> getSubscriptions() {
		return subscriptions;
	}
}
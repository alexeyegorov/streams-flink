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

import java.io.ByteArrayInputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import stream.Data;
import stream.Processor;
import stream.ProcessorList;
import stream.FlinkRunner;
import stream.Subscription;
import stream.data.DataFactory;
import stream.io.Sink;
import stream.runtime.setup.factory.ObjectFactory;
import stream.runtime.setup.factory.ProcessorFactory;
import stream.util.Variables;

/**
 * <p>
 * This bolt implementation wraps a process configuration and creates the
 * process including its inner element to provide a bolt that behaves the same
 * as a regular streams process.
 * </p>
 * <p>
 * TODO: Currently, services are not injected into bolts as there is no service
 * facade implementation for flink, yet.
 * </p>
 * 
 * @author Christian Bockermann
 * 
 */
public class ProcessBolt extends AbstractBolt {

	/** The unique class ID */
	private static final long serialVersionUID = -924312414467186051L;

	static Logger log = LoggerFactory.getLogger(ProcessBolt.class);

	transient ProcessorList process;

	// a list of the output references for each of the processors within this
	// process
	transient List<OutputRef> outputRefs = new ArrayList<OutputRef>();

	protected final Variables variables;
	protected String[] outputs;
	final BoltContext ctx = new BoltContext();

	/**
	 * The list of subscribers (e.g. output queues,...) that need to be
	 * connected to this bolt
	 */
	final Set<Subscription> subscriptions = new LinkedHashSet<Subscription>();

	/**
	 * The bolt implementation requires an XML configuration (the complete
	 * container XML as string) and the ID that identifies the corresponding
	 * process within that XML.
	 * 
	 * @param xmlConfig
	 *            The XML configuration as String.
	 * @param uuid
	 *            The ID of the process.
	 */
	public ProcessBolt(String xmlConfig, String uuid, Map<String, String> variables) throws Exception {
		super(xmlConfig, uuid);
		this.variables = new Variables(variables);

		// first step: instantiate the process and all of its
		// processors - this is required to determine any references to
		// sinks/services
		//
		createProcess();
	}

	@Override
	public void declareOutputFields(final OutputFieldsDeclarer outputFieldsDeclarer) {

		super.declareOutputFields(outputFieldsDeclarer);

		for (Subscription sub : getSubscriptions()) {
			log.info("Declaring output stream '{}'", sub.subscriber());
			outputFieldsDeclarer.declareStream(sub.subscriber(), new Fields(TupleWrapper.DATA_KEY));
		}
	}

	/**
	 * This method returns the list (set) of queues ('sinks') that are
	 * referenced by any of the processors of this process bolt. These need to
	 * artificially be connected to this bolt (subscription model).
	 * 
	 * @return
	 */
	public Set<Subscription> getSubscriptions() {
		return subscriptions;
	}

	/**
	 * This method creates the inner processors of this process bolt.
	 * 
	 * @return
	 * @throws Exception
	 */
	protected ProcessorList createProcess() throws Exception {

		DocumentBuilder builder = DocumentBuilderFactory.newInstance().newDocumentBuilder();
		Document config = builder.parse(new ByteArrayInputStream(xmlConfig.getBytes()));

		Element element = FlinkRunner.findElementByUUID(config.getDocumentElement(), uuid);

		if (element == null) {
			log.error("Failed to find process for uuid '{}' in the XML!", uuid);
			throw new Exception("Failed to find process for uuid '" + uuid + "' in the XML!");
		}

		ObjectFactory obf = ObjectFactory.newInstance();
		obf.addVariables(this.variables);
		ProcessorFactory pf = new ProcessorFactory(obf);

		// The handler injects wrappers for any QueueService accesses, thus
		// effectively doing the queue-flow injection
		//
		QueueInjection queueInjection = new QueueInjection(uuid, output);
		pf.addCreationHandler(queueInjection);

		log.debug("Creating processor-list from element {}", element);
		List<Processor> list = pf.createNestedProcessors(element);

		process = new ProcessorList();
		for (Processor p : list) {
			process.getProcessors().add(p);
		}

		if (element.hasAttribute("output")) {
			String out = element.getAttribute("output");
			if (out.indexOf(",") > 0) {
				outputs = out.split(",");
			} else {
				outputs = new String[] { out };
			}
		}

		subscriptions.addAll(queueInjection.getSubscriptions());
		log.debug("Found {} subscribers for bolt '{}': " + subscriptions, subscriptions.size(), uuid);
		return process;
	}

	public List<Processor> getAllProcessors() {
		return getAllProcessors(process);
	}

	protected List<Processor> getAllProcessors(ProcessorList list) {
		List<Processor> ps = new ArrayList<Processor>();

		for (Processor p : list.getProcessors()) {
			if (p instanceof ProcessorList) {
				ps.addAll(getAllProcessors((ProcessorList) p));
			} else {
				ps.add(p);
			}
		}
		return ps;
	}

	protected Processor createProcessor(Element el, ProcessorFactory pf) throws Exception {

		Processor p = pf.createProcessor(el);

		if (p instanceof ProcessorList) {
			ProcessorList list = (ProcessorList) p;

			NodeList nested = el.getChildNodes();
			for (int i = 0; i < nested.getLength(); i++) {
				Node ch = nested.item(i);
				if (ch.getNodeType() == Node.ELEMENT_NODE) {
					Element e = (Element) ch;
					Processor inner = createProcessor(e, pf);
					list.getProcessors().add(inner);
				}
			}
		}

		return p;
	}

	/**
	 * @see backtype.storm.task.IBolt#prepare(java.util.Map,
	 *      backtype.storm.task.TopologyContext,
	 *      backtype.storm.task.OutputCollector)
	 */
	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {

		log.debug("Preparing ProcessBolt {}", this.uuid);
		this.output = collector;
		log.debug("   output collector: {}", output);

		try {

			process = createProcess();
			process.init(ctx);

		} catch (Exception e) {
			e.printStackTrace();
			throw new RuntimeException("Failed to create process!");
		}
	}

	/**
	 * @see backtype.storm.task.IBolt#execute(backtype.storm.tuple.Tuple)
	 */
	@Override
	public void execute(Tuple input) {
		log.debug("Tuple received: {}", input);

		Data item = null;

		try {
			Object data = input.getValueByField("stream.Data");
			log.debug("Data is: {}", data);
			if (data != null) {
				item = (Data) data;
			}
		} catch (Exception e) {
			log.debug("Error processing tuple: {}", e.getMessage());

			item = DataFactory.create();
			Fields fields = input.getFields();
			for (int i = 0; i < fields.size(); i++) {
				String key = fields.get(i);
				Object value = input.getValue(i);
				if (value instanceof Serializable) {
					item.put(key, (Serializable) value);
				}
			}
		}

		if (item != null) {
			log.debug("Processing item...");
			item = process.process(item);

			if (outputs != null) {
				for (String out : outputs) {
					log.debug("Emitting result item to {}: {}", out, item);
					output.emit(out, new Values(item));
				}
			} else {
				log.debug("Emitting item {}", item);
				output.emit(new Values(item));
			}
		} else {
			log.debug("No item to process!");
		}
	}

	public final class DataForwarder implements Sink {

		String id;
		final OutputCollector output;

		public DataForwarder(String id, OutputCollector output) {
			this.id = id;
			this.output = output;
		}

		/**
		 * @see stream.io.Sink#getId()
		 */
		@Override
		public String getId() {
			return id;
		}

		/**
		 * @see stream.io.Sink#write(stream.Data)
		 */
		@Override
		public boolean write(Data item) throws Exception {
			if (item == null)
				return false;
			output.emit(id, new Values(item));
			return true;
		}

		@Override
		public void close() throws Exception {
		}

		@Override
		public boolean write(Collection<Data> data) throws Exception {

			for (Data item : data) {
				output.emit(id, new Values(item));
			}

			return true;
		}

		/**
		 * @see stream.io.Sink#setId(java.lang.String)
		 */
		@Override
		public void setId(String id) {
			this.id = id;
		}

		/*
		 * (non-Javadoc)
		 * 
		 * @see stream.io.Sink#init()
		 */
		@Override
		public void init() throws Exception {
			// TODO Auto-generated method stub

		}
	}

	public final class OutputRef {
		final Processor processor;
		final String property;
		final String[] refs;

		public OutputRef(Processor p, String property, String[] refs) {
			this.processor = p;
			this.property = property;
			this.refs = refs;
		}
	}

	/**
	 * @see backtype.storm.topology.base.BaseRichBolt#cleanup()
	 */
	@Override
	public void cleanup() {
		super.cleanup();

		try {
			log.debug("Finishing process {}", uuid);
			process.finish();
		} catch (Exception e) {
			log.error("Failed to properly shutdown process {}: {}", this.uuid, e.getMessage());
		}
	}

}
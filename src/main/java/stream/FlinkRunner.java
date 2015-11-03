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
package stream;

import org.apache.flink.storm.api.FlinkLocalCluster;
import org.apache.flink.storm.api.FlinkTopology;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.StringWriter;
import java.util.UUID;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.transform.Result;
import javax.xml.transform.Source;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import backtype.storm.Config;
import backtype.storm.utils.Utils;
import stream.util.XMLUtils;

/**
 * @author chris & alexey
 */
public class FlinkRunner {

    static Logger log = LoggerFactory.getLogger(FlinkRunner.class);
    public final static String UUID_ATTRIBUTE = "id";

    public static void addUUIDAttributes(Element element) {

        String theId = element.getAttribute("id");
        if (theId == null || theId.trim().isEmpty()) {
            UUID id = UUID.randomUUID();
            element.setAttribute(UUID_ATTRIBUTE, id.toString());
        }

        NodeList list = element.getChildNodes();
        for (int i = 0; i < list.getLength(); i++) {
            Node node = list.item(i);
            if (node.getNodeType() == Node.ELEMENT_NODE) {
                addUUIDAttributes((Element) node);
            }
        }
    }

    public static String createIDs(InputStream in) throws Exception {

        DocumentBuilder builder = DocumentBuilderFactory.newInstance().newDocumentBuilder();
        Document doc = builder.parse(in);

        addUUIDAttributes(doc.getDocumentElement());

        Transformer trans = TransformerFactory.newInstance().newTransformer();
        Source source = new DOMSource(doc);
        StringWriter out = new StringWriter();
        Result output = new StreamResult(out);
        trans.transform(source, output);

        String xml = out.toString();
        return xml;
    }

    public static Element findElementByUUID(Element el, String uuid) {
        String id = el.getAttribute(UUID_ATTRIBUTE);
        if (uuid.equals(id)) {
            return el;
        }

        NodeList list = el.getChildNodes();
        for (int i = 0; i < list.getLength(); i++) {
            Node node = list.item(i);
            if (node.getNodeType() == Node.ELEMENT_NODE) {

                Element found = findElementByUUID((Element) node, uuid);
                if (found != null)
                    return found;
            }
        }

        return null;
    }

    /**
     * @param args
     */
    public static void main(String[] args) throws Exception {

        InputStream in = null;
        if (args.length > 0) {
            File xml = new File(args[0]);
            in = new FileInputStream(xml);
        } else {
            in = FlinkRunner.class.getResourceAsStream("/example.xml");
        }

        long start = System.currentTimeMillis();
        String xml = createIDs(in);
        long end = System.currentTimeMillis();

        log.info("Creating XML took {}", (end - start));
        log.info("XML result is:\n{}", xml);

        Document doc = DocumentBuilderFactory.newInstance()
                .newDocumentBuilder()
                .parse(new ByteArrayInputStream(xml.getBytes()));

        doc = XMLUtils.parseDocument(xml);
        doc = XMLUtils.addUUIDAttributes(doc, UUID_ATTRIBUTE);

        log.info("Encoding document...");
        String enc = DocumentEncoder.encodeDocument(doc);
        log.info("Arg will be:\n{}", enc);

        Document decxml = DocumentEncoder.decodeDocument(enc);
        log.info("Decoded XML is: {}", XMLUtils.toString(decxml));

        if (enc == null)
            return;

        Config conf = new Config();
        conf.setDebug(false);

        StreamTopology st = StreamTopology.create(doc);

        log.info("Creating stream-topology...");

        FlinkTopology storm = st.createTopology();

        log.info("Starting local cluster...");
        FlinkLocalCluster cluster = new FlinkLocalCluster();

        log.info("########################################################################");
        log.info("submitting topology...");
        cluster.submitTopology("test", conf, storm);
        log.info("########################################################################");

        log.info("Topology submitted.");
        Utils.sleep(10000000);

        log.info("########################################################################");
        log.info("killing topology...");
        cluster.killTopology("test");
        cluster.shutdown();
    }
}
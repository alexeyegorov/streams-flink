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

import org.apache.flink.storm.api.FlinkSubmitter;
import org.apache.flink.storm.api.FlinkTopology;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;

import backtype.storm.Config;
import stream.StreamTopology;
import stream.runtime.StreamRuntime;
import stream.storm.Constants;
import stream.util.XMLUtils;

/**
 * @author chris
 */
public class deploy {

    static Logger log = LoggerFactory.getLogger(deploy.class);

    public static Set<String> getRequiredOptions() {
        Set<String> opts = new LinkedHashSet<String>();
        opts.add(Config.NIMBUS_HOST);
        return Collections.unmodifiableSet(opts);
    }

    public static List<String> handleArgs(String[] args, final Properties p)
            throws IOException {

        File userProps = new File(System.getProperty("user.home")
                + File.separator + ".streams.properties");

        if (userProps.canRead()) {
            System.out.println("Reading user-properties from "
                    + userProps.getAbsolutePath() + ":");
            for (Object k : p.keySet()) {
                System.out.println("  " + k + " = "
                        + p.getProperty(k.toString()));
            }

            p.load(new FileInputStream(userProps));
            System.getProperties().putAll(p);
        }

        if (args.length > 2) {
            System.err.println("Expecting exactly a single XML configuration file " +
                    "and an optional time value parameter.");
            System.exit(-1);
        }

        List<String> params = new ArrayList<String>();
        for (String arg : args) {
            params.add(arg);
        }

        Iterator<String> it = params.iterator();
        while (it.hasNext()) {

            String param = it.next();
            if (param.startsWith("-")) {

                if (param.startsWith("--")) {
                    param = param.substring(2);
                } else {
                    param = param.substring(1);
                }

                if (param.indexOf("=") > 0) {
                    String[] kv = param.split("=", 2);
                    System.setProperty(kv[0], kv[1]);
                } else {
                    System.setProperty(param, "true");
                }

                it.remove();
            }
        }

        return params;
    }

    /**
     * @param args
     */
    public static void main(String[] args) {
        try {
            stream.runtime.StreamRuntime.loadUserProperties();
            StreamRuntime.setupLogging();
            final Properties p = new Properties();
            p.putAll(System.getProperties());
            final List<String> params = handleArgs(args, p);

            Config config = new Config();

            log.info("Building sub-topology...");

            for (Object k : p.keySet()) {
                log.info("Adding property   {} to flink config (value: '{}')",
                        k.toString(), p.getProperty(k.toString()));
                config.put(k.toString(), p.getProperty(k.toString()));
            }

            int err = 0;
            for (String opt : getRequiredOptions()) {
                if (!config.containsKey(opt)) {
                    log.error("Required parameter '{}' not set!", opt);
                    err++;
                }
            }

            if (err > 0) {
                log.error("{} required parameters missing.", err);
                System.exit(-1);
            }

            if (params.isEmpty()) {
                System.err.println("You need to specify an XML configuration!");
                System.exit(-1);
            }

            URL url = new File(params.get(0)).toURI().toURL();
            log.info("Creating topology with config from '{}'", url);

            Document doc = XMLUtils.parseDocument(url.openStream());
            Element root = doc.getDocumentElement();
            String id = root.getAttribute("id");
            log.info("Container/topology ID is: '{}'", id);

            // retrieve number of workers to be used
            String numWorkers = root.getAttribute(Constants.NUM_WORKERS);
            if (!numWorkers.equals("")) {
                log.info("Topology should use {} workers.", numWorkers);
                int workers = Integer.valueOf(numWorkers);
                config.setNumWorkers(workers);
            }

            StreamTopology topology = StreamTopology.build(doc,
                    StreamTopologyBuilder.createFlinkTopologyBuilder());

            String name = id;
            if (id == null || id.trim().isEmpty()) {
                name = UUID.randomUUID().toString().toLowerCase();
            }

            log.info("Submitting topology '{}'", name);
            FlinkSubmitter.submitTopology(name, config, topology.createFlinkTopology());

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
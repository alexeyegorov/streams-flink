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

import junit.framework.Assert;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URL;

import stream.test.Collector;

import static org.junit.Assert.fail;

/**
 * @author chris
 */
public class QueueTest {

    static Logger log = LoggerFactory.getLogger(QueueTest.class);

    @Test
    public void test() {
        try {

            final URL url = QueueTest.class.getResource("/storm-queues.xml");
            log.info("Running flink topology from {}", url);
            Thread t = new Thread() {
                public void run() {
                    try {
                        flink.run.main(new String[]{url.getPath()});
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            };
            t.start();

            int waitingTimes = 10;
            while (Collector.getCollection().size() < 1000 & waitingTimes-- > 0) {
                log.info("{} items collected, waiting...", Collector.getCollection().size());
                Thread.sleep(1000);
            }

            log.info("{} items collected.", Collector.getCollection().size());
            flink.run.stopLocalCluster();

            Assert.assertEquals(1000, Collector.getCollection().size());

        } catch (Exception e) {
            e.printStackTrace();
            fail("Not yet implemented");
        }
    }
}

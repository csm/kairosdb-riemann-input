package org.metastatic.kairosdb.riemann;

import com.aphyr.riemann.Proto;
import com.aphyr.riemann.client.RiemannClient;
import com.aphyr.riemann.client.ServerError;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.kairosdb.core.DataPointListener;
import org.kairosdb.core.datapoints.DoubleDataPointFactoryImpl;
import org.kairosdb.core.datapoints.LongDataPointFactoryImpl;
import org.kairosdb.core.datastore.KairosDatastore;
import org.kairosdb.core.datastore.QueryQueuingManager;
import org.kairosdb.core.exception.DatastoreException;
import org.kairosdb.core.exception.KairosDBException;
import org.kairosdb.datastore.h2.H2Datastore;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.Collections;
import java.util.LinkedList;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;

public class RiemannTcpServerTest {
    private RiemannClient client;
    private int port;
    private RiemannTcpServer tcpServer;
    private KairosDatastore datastore;
    private MemoryDatastore memDatastore;

    @Before
    public void setUp() throws KairosDBException, IOException, InterruptedException {
        port = 31337; //new Random().nextInt(1024) + 30000;
        memDatastore = new MemoryDatastore();
        datastore = new KairosDatastore(memDatastore, new QueryQueuingManager(1, "127.0.0.1"), Collections.<DataPointListener>emptyList(), null);
        tcpServer = new RiemannTcpServer("127.0.0.1", port, "127.0.0.1", datastore, new LongDataPointFactoryImpl(), new DoubleDataPointFactoryImpl());
        tcpServer.start();
        client = RiemannClient.tcp("127.0.0.1", port);
        for (int i = 0; i < 10; i++) {
            try {
                client.connect();
                break;
            } catch (Exception x) {
                Thread.sleep(500);
            }
        }
    }

    @After
    public void tearDown() {
        client.close();
        tcpServer.stop();
    }

    @Test
    public void testMessageD() throws IOException {
        Proto.Msg result = client.event().service("test.riemann")
                .metric(3.14159d)
                .host("localhost")
                .time(System.currentTimeMillis())
                .attribute("foo", "bar")
                .attribute("bar", "baz")
                .tag("oldstyle:tags")
                .send().deref(1, TimeUnit.SECONDS);
        assertNotNull(result);
        assertTrue(result.getOk());
        LinkedList<MemoryDatastore.Entry> list = memDatastore.dataPoints.get("test.riemann");
        assertNotNull(list);
        assertEquals(1, list.size());
        assertEquals(3.14159, list.get(0).dataPoint.getDoubleValue(), 0.01);
        assertEquals("localhost", list.get(0).tags.get("host"));
        assertEquals("bar", list.get(0).tags.get("foo"));
        assertEquals("baz", list.get(0).tags.get("bar"));
        assertEquals("tags", list.get(0).tags.get("oldstyle"));
    }

    @Test
    public void testMessageL() throws IOException {
        Proto.Msg result = client.event().service("test.riemann")
                .metric(31337L)
                .host("localhost")
                .time(System.currentTimeMillis())
                .attribute("foo", "bar")
                .attribute("bar", "baz")
                .tag("oldstyle:tags")
                .send().deref(1, TimeUnit.SECONDS);
        assertNotNull(result);
        assertTrue(result.getOk());
        LinkedList<MemoryDatastore.Entry> list = memDatastore.dataPoints.get("test.riemann");
        assertNotNull(list);
        assertEquals(1, list.size());
        assertEquals(31337L, list.get(0).dataPoint.getLongValue());
        assertEquals("localhost", list.get(0).tags.get("host"));
        assertEquals("bar", list.get(0).tags.get("foo"));
        assertEquals("baz", list.get(0).tags.get("bar"));
        assertEquals("tags", list.get(0).tags.get("oldstyle"));
    }

    @Test
    public void testMessageNoMetric() throws IOException {
        try {
            Proto.Msg result = client.event().service("test.riemann")
                    .host("localhost")
                    .time(System.currentTimeMillis())
                    .attribute("foo", "bar")
                    .attribute("bar", "baz")
                    .tag("oldstyle:tags")
                    .send().deref(1, TimeUnit.SECONDS);
            fail();
        } catch (ServerError e) {
            // pass
        }
    }
}

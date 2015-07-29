package org.metastatic.kairosdb.riemann;

import com.google.common.collect.ImmutableSortedMap;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.*;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;
import org.jboss.netty.handler.codec.frame.LengthFieldBasedFrameDecoder;
import org.jboss.netty.handler.codec.frame.LengthFieldPrepender;
import org.jboss.netty.handler.codec.protobuf.ProtobufDecoder;
import org.jboss.netty.handler.codec.protobuf.ProtobufEncoder;
import org.jboss.netty.handler.execution.ExecutionHandler;
import org.jboss.netty.handler.execution.OrderedMemoryAwareThreadPoolExecutor;
import org.kairosdb.core.DataPoint;
import org.kairosdb.core.DataPointSet;
import org.kairosdb.core.KairosDBService;
import org.kairosdb.core.datapoints.DoubleDataPointFactory;
import org.kairosdb.core.datapoints.LongDataPointFactory;
import org.kairosdb.core.datastore.KairosDatastore;
import org.kairosdb.core.exception.KairosDBException;
import org.kairosdb.core.reporting.KairosMetricReporter;
import org.kairosdb.util.Tags;
import org.kairosdb.util.ValidationException;
import org.metastatic.shaded.com.aphyr.riemann.Proto.Attribute;
import org.metastatic.shaded.com.aphyr.riemann.Proto.Event;
import org.metastatic.shaded.com.aphyr.riemann.Proto.Msg;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

public class RiemannTcpServer extends SimpleChannelUpstreamHandler implements ChannelPipelineFactory, KairosDBService, KairosMetricReporter {
    private static final String REPORTING_METRIC_REQUESTS_NAME = "kairosdb.protocol.riemann_request_count";
    private static final String REPORTING_METRIC_EVENTS_NAME = "kairosdb.protocol.riemann_event_count";
    static final Logger logger = LoggerFactory.getLogger(RiemannTcpServer.class);
    private final InetAddress address;
    private final int port;
    private final AtomicInteger eventCount = new AtomicInteger();
    private final AtomicInteger messageCount = new AtomicInteger();
    private final KairosDatastore datastore;
    private final LongDataPointFactory longDataPointFactory;
    private final DoubleDataPointFactory doubleDataPointFactory;
    private final String hostname;
    private ServerBootstrap serverBootstrap;

    private Msg success = Msg.newBuilder().setOk(true).build();
    private Msg failure = Msg.newBuilder().setOk(false).buildPartial();

    @Inject
    public RiemannTcpServer(@Named("kairosdb.riemannserver.address") String address,
                            @Named("kairosdb.riemannserver.port") int port,
                            @Named("HOSTNAME") String hostname,
                            KairosDatastore datastore,
                            LongDataPointFactory longDataPointFactory,
                            DoubleDataPointFactory doubleDataPointFactory) throws UnknownHostException {
        this.address = InetAddress.getByName(address);
        this.port = port;
        this.hostname = hostname;
        this.datastore = datastore;
        this.longDataPointFactory = longDataPointFactory;
        this.doubleDataPointFactory = doubleDataPointFactory;
    }

    public ChannelPipeline getPipeline() throws Exception {
        ChannelPipeline pipeline = Channels.pipeline();
        pipeline.addLast("frameDecoder", new LengthFieldBasedFrameDecoder(524288, 0, 4, 0, 4));
        pipeline.addLast("frameEncoder", new LengthFieldPrepender(4));
        pipeline.addLast("executor", new ExecutionHandler(new OrderedMemoryAwareThreadPoolExecutor(16, 1048576, 10485760)));
        pipeline.addLast("decoder", new ProtobufDecoder(Msg.getDefaultInstance()));
        pipeline.addLast("encoder", new ProtobufEncoder());
        pipeline.addLast("handler", this);
        return pipeline;
    }

    @Override
    public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) throws Exception {
        final Object message = e.getMessage();
        if (message instanceof Msg) {
            Msg msg = (Msg) message;
            for (Event event : msg.getEventsList()) {
                DataPoint dp;
                if (event.hasMetricD()) {
                    dp = doubleDataPointFactory.createDataPoint(event.getTime() * 1000, event.getMetricD());
                } else if (event.hasMetricF()) {
                    dp = doubleDataPointFactory.createDataPoint(event.getTime() * 1000, (double) event.getMetricF());
                } else if (event.hasMetricSint64()) {
                    dp = longDataPointFactory.createDataPoint(event.getTime() * 1000, event.getMetricSint64());
                } else {
                    throw new ValidationException("event received with no metrics, ignoring");
                }

                ImmutableSortedMap.Builder<String, String> tags = Tags.create();
                for (Attribute attribute : event.getAttributesList()) {
                    if (attribute.hasKey() && attribute.hasValue()) {
                        tags.put(attribute.getKey(), attribute.getValue());
                    }
                }
                for (String tag : event.getTagsList()) {
                    String[] parts = tag.split(":", 2);
                    if (parts.length == 2 && !parts[0].isEmpty() && !parts[1].isEmpty()) {
                        tags.put(parts[0], parts[1]);
                    } else {
                        logger.info("skipping invalid tag {}; expected 'key:value' format", tag);
                    }
                }
                tags.put("host", event.getHost());
                eventCount.incrementAndGet();
                datastore.putDataPoint(event.getService(), tags.build(), dp);
            }
            messageCount.incrementAndGet();
            logger.debug("wrote off all events, sending ok");
            e.getChannel().write(success);
        } else {
            throw new ValidationException("expected a Proto.Msg, got " + ((message != null) ? "a " + message.getClass().getName() : "null"));
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) throws Exception {
        try {
            e.getChannel().write(Msg.newBuilder(failure).setError(e.getCause().toString()).build());
        } catch (Exception ex) {
            logger.warn("exception caught in exception handler, bailing", ex);
            try {
                e.getChannel().close();
            } catch (Exception ex2) {
                logger.warn("exception trynig to close channel", ex2);
            }
        }
    }

    public void start() throws KairosDBException {
        serverBootstrap = new ServerBootstrap(new NioServerSocketChannelFactory(Executors.newCachedThreadPool(), Executors.newCachedThreadPool()));
        serverBootstrap.setPipelineFactory(this);
        serverBootstrap.setOption("child.tcpNoDelay", true);
        serverBootstrap.setOption("child.keepAlive", true);
        serverBootstrap.setOption("reuseAddress", true);
        serverBootstrap.bind(new InetSocketAddress(address, port));
    }

    public void stop() {
        if (serverBootstrap != null) {
            serverBootstrap.shutdown();
            serverBootstrap = null;
        }
    }

    public List<DataPointSet> getMetrics(long now) {
        DataPointSet requests = new DataPointSet(REPORTING_METRIC_REQUESTS_NAME);
        requests.addTag("host", hostname);
        requests.addDataPoint(longDataPointFactory.createDataPoint(now, messageCount.getAndSet(0)));
        DataPointSet events = new DataPointSet(REPORTING_METRIC_EVENTS_NAME);
        events.addTag("host", hostname);
        events.addDataPoint(longDataPointFactory.createDataPoint(now, eventCount.getAndSet(0)));
        return Arrays.asList(requests, events);
    }
}

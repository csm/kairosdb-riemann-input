package org.metastatic.kairosdb.riemann;

import com.aphyr.riemann.Proto;
import com.google.common.collect.ImmutableMap;
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
import org.kairosdb.core.DataPoint;
import org.kairosdb.core.DataPointSet;
import org.kairosdb.core.KairosDBService;

import com.aphyr.riemann.Proto.*;
import org.kairosdb.core.datapoints.DoubleDataPoint;
import org.kairosdb.core.datapoints.DoubleDataPointFactory;
import org.kairosdb.core.datapoints.LongDataPointFactory;
import org.kairosdb.core.datastore.KairosDatastore;
import org.kairosdb.core.exception.KairosDBException;
import org.kairosdb.core.reporting.KairosMetricReporter;
import org.kairosdb.util.Tags;
import org.kairosdb.util.ValidationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

public class RiemannTcpServer extends SimpleChannelUpstreamHandler implements ChannelPipelineFactory, KairosDBService, KairosMetricReporter {
    private static final String REPORTING_METRIC_NAME = "kairosdb.protocol.riemann_request_count";
    static final Logger logger = LoggerFactory.getLogger(RiemannTcpServer.class);
    private final InetAddress address;
    private final int port;
    private final AtomicInteger messageCount = new AtomicInteger();
    private final KairosDatastore datastore;
    private final LongDataPointFactory longDataPointFactory;
    private final DoubleDataPointFactory doubleDataPointFactory;
    private final String hostname;
    private ServerBootstrap serverBootstrap;

    private Msg success = Msg.newBuilder().setOk(true).build();
    private Msg failure = Msg.newBuilder().setOk(false).buildPartial();

    @Inject
    public RiemannTcpServer(@Named("kairosdb.riemannprotobuf.address") String address,
                            @Named("kairosdb.riemannprotobuf.port") int port,
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
        pipeline.addLast("frameDecoder", new LengthFieldBasedFrameDecoder(65536, 0, 4, 0, 4));
        pipeline.addLast("frameEncoder", new LengthFieldPrepender(4));
        pipeline.addLast("decoder", new ProtobufDecoder(Proto.Msg.getDefaultInstance()));
        pipeline.addLast("encoder", new ProtobufEncoder());
        pipeline.addLast("handler", this);
        return pipeline;
    }

    @Override
    public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) throws Exception {
        final Object message = e.getMessage();
        if (message instanceof Msg) {
            Msg msg = (Msg) message;
            logger.debug("got message: {}", msg);
            for (Event event : msg.getEventsList()) {
                DataPoint dp;
                if (event.hasMetricD()) {
                    dp = doubleDataPointFactory.createDataPoint(event.getTime(), event.getMetricD());
                } else if (event.hasMetricF()) {
                    dp = doubleDataPointFactory.createDataPoint(event.getTime(), (double) event.getMetricF());
                } else if (event.hasMetricSint64()) {
                    dp = longDataPointFactory.createDataPoint(event.getTime(), event.getMetricSint64());
                } else {
                    throw new ValidationException("event received with no metrics, ignoring");
                }

                ImmutableSortedMap.Builder<String, String> tags = Tags.create();
                for (Attribute attribute : event.getAttributesList()) {
                    tags.put(attribute.getKey(), attribute.getValue());
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
                messageCount.incrementAndGet();
                datastore.putDataPoint(event.getService(), tags.build(), dp);
            }
            logger.debug("wrote off all events, sending ok");
            e.getChannel().write(success);
        } else {
            throw new ValidationException("expected a Proto.Msg, got " + ((message != null) ? "a " + message.getClass().getName() : "null"));
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) throws Exception {
        e.getChannel().write(Msg.newBuilder(failure).setError(e.getCause().toString()).build());
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
        DataPointSet dps = new DataPointSet(REPORTING_METRIC_NAME);
        dps.addTag("host", hostname);
        dps.addDataPoint(longDataPointFactory.createDataPoint(now, messageCount.getAndSet(0)));
        return Collections.singletonList(dps);
    }
}

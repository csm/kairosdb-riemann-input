package org.metastatic.kairosdb.riemann;

import com.google.inject.AbstractModule;
import com.google.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RiemannModule extends AbstractModule {
    public static final Logger logger = LoggerFactory.getLogger(RiemannModule.class);

    @Override
    protected void configure() {
        logger.info("configuring Riemann protobuf input module");
        bind(RiemannTcpServer.class).in(Singleton.class);
    }
}

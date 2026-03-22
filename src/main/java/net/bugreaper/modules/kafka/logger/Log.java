package net.bugreaper.modules.kafka.logger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class Log {
    public static final Logger LOGGER = LoggerFactory.getLogger("bugreaper-module-kafka");

    private Log() {
        throw new IllegalStateException("Utility class");
    }

}
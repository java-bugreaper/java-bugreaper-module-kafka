package net.bugreaper.modules.kafka.logger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility class for handling logging within the Kafka module.
 * This class provides a centralized logger for consistent logging across the module.
 */
public final class Log {

    /**
     * Logger instance for the Kafka module.
     * Used to log events related to Kafka operations within the module.
     */
    public static final Logger LOGGER = LoggerFactory.getLogger("bugreaper-module-kafka");

    private Log() {
        throw new IllegalStateException("Utility class");
    }

}
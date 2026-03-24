package net.bugreaper.modules.kafka.exceptions;

/**
 *  Exception for Kafka helper
 */
public class KafkaHelperException extends RuntimeException {

    /**
     * Basic exception for Kafka helper
     *
     * @param cause trace
     */
    public KafkaHelperException(Throwable cause) {
        super(cause);
    }

    /**
     * Basic exception for Kafka helper
     *
     * @param message info
     * @param cause trace
     */
    public KafkaHelperException(String message, Throwable cause) {
        super(message, cause);
    }

    /**
     * Basic exception for Kafka helper
     *
     * @param message info
     */
    public KafkaHelperException(String message) {
        super(message);
    }
}

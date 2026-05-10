package net.bugreaper.modules.kafka.exceptions;

/**
 *  Exception for Kafka helper
 */
public class KafkaHelperException extends RuntimeException {

    /**
     * Basic exception for Kafka helper
     *
     * @param cause The underlying cause that triggered this exception
     */
    public KafkaHelperException(Throwable cause) {
        super(cause);
    }

    /**
     * Basic exception for Kafka helper
     *
     * @param message A descriptive message explaining the error
     * @param cause The underlying cause that triggered this exception
     */
    public KafkaHelperException(String message, Throwable cause) {
        super(message, cause);
    }

    /**
     * Basic exception for Kafka helper
     *
     * @param message A descriptive message explaining the error
     */
    public KafkaHelperException(String message) {
        super(message);
    }
}

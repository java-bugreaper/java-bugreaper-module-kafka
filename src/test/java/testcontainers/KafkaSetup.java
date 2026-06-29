package testcontainers;

import com.github.dockerjava.api.model.ExposedPort;
import com.github.dockerjava.api.model.PortBinding;
import com.github.dockerjava.api.model.Ports;
import net.bugreaper.modules.kafka.Kafka;

import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.util.Objects;


public class KafkaSetup {

    private static final DockerImageName KAFKA_TEST_IMAGE = DockerImageName.parse("apache/kafka:4.1.1");

    static KafkaContainer container = new KafkaContainer(KAFKA_TEST_IMAGE)
            .withStartupTimeout(Duration.ofSeconds(30))
            .withCreateContainerCmdModifier(cmd -> Objects.requireNonNull(cmd.getHostConfig()).withPortBindings(
                    new PortBinding(Ports.Binding.bindPort(9096), new ExposedPort(9092))
            ))
            .waitingFor(
                    Wait.forLogMessage(".*Kafka Server started.*", 1));


    private static KafkaSetup instance;

    public KafkaSetup() {
        container.start();
    }

    public static synchronized KafkaSetup getInstance() {
        if (instance == null) {
            instance = new KafkaSetup();
        }

        return instance;
    }

    public synchronized Kafka getKafka() {
        return new Kafka(
                container.getHost() + ":" + container.getMappedPort(9092)
        );
    }

}

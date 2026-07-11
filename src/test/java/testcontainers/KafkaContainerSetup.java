package testcontainers;

import com.github.dockerjava.api.model.ExposedPort;
import com.github.dockerjava.api.model.PortBinding;
import com.github.dockerjava.api.model.Ports;
import net.bugreaper.modules.kafka.Kafka;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.kafka.KafkaContainer;

import java.time.Duration;
import java.util.Objects;


public abstract class KafkaContainerSetup {

    static KafkaContainer container = new KafkaContainer("apache/kafka:4.1.1")
            .withStartupTimeout(Duration.ofSeconds(30))
            .withCreateContainerCmdModifier(cmd -> Objects.requireNonNull(cmd.getHostConfig()).withPortBindings(
                    new PortBinding(Ports.Binding.bindPort(9096), new ExposedPort(9092))
            ))
            .waitingFor(
                    Wait.forLogMessage(".*Kafka Server started.*", 1));

    static {
        container.start();
    }

    public static Kafka getKafka() {
        return new Kafka(
                container.getHost() + ":" + container.getMappedPort(9092)
        );
    }

}

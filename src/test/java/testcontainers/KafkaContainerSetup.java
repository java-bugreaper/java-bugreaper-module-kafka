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

    private static final String STABLE_VERSION = "apache/kafka:3.9.1";
    private static final String LATEST_VERSION = "apache/kafka:4.3.1";

    private static final String DOCKER_IMAGE = resolveDockerImage();


    static KafkaContainer container = new KafkaContainer(DOCKER_IMAGE)
            .withStartupTimeout(Duration.ofSeconds(30))
            .withCreateContainerCmdModifier(cmd -> Objects.requireNonNull(cmd.getHostConfig()).withPortBindings(
                    new PortBinding(Ports.Binding.bindPort(9096), new ExposedPort(9092))
            ))
            .waitingFor(
                    Wait.forLogMessage(".*Kafka Server started.*", 1));

    static {
        System.out.printf("""
                \u001B[32m
                ============================================
                >>> TESTS RUNNING ON ON DOCKER IMAGE: %s <<<
                ============================================
                \u001B[0m
                %n""", DOCKER_IMAGE);

        container.start();
    }

    private static String resolveDockerImage() {
        String dockerVersion = System.getProperty("dockerTestVersion");

        if ("latest".equalsIgnoreCase(dockerVersion)) {
            return LATEST_VERSION;
        }

        return STABLE_VERSION;
    }

    public static Kafka getKafka() {
        return new Kafka(
                container.getHost() + ":" + container.getMappedPort(9092)
        );
    }

}

package net.bugreaper.modules.kafka;

import org.junit.jupiter.api.Test;

import static net.bugreaper.core.utils.AllureStepsValidator.validateAllSteps;

class AllureStepsValidationTest {

    @Test
    void testStepsKafka() {
        validateAllSteps("net.bugreaper.modules.kafka.Kafka");
    }

}

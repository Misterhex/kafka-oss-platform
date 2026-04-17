package com.example.producer;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.stereotype.Component;

@Component("kafkaLiveness")
public class KafkaLivenessHealthIndicator implements HealthIndicator {

    private final SensorProducerRunner runner;
    private final long thresholdMs;

    public KafkaLivenessHealthIndicator(
            SensorProducerRunner runner,
            @Value("${synthetic.liveness.staleness-threshold-seconds:30}") long thresholdSeconds) {
        this.runner = runner;
        this.thresholdMs = thresholdSeconds * 1000L;
    }

    @Override
    public Health health() {
        long stalenessMs = runner.millisSinceLastSuccess();
        Health.Builder builder = stalenessMs > thresholdMs ? Health.down() : Health.up();
        return builder
                .withDetail("stalenessMs", stalenessMs)
                .withDetail("thresholdMs", thresholdMs)
                .build();
    }
}

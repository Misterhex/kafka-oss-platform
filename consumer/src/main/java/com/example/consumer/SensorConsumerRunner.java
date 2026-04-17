package com.example.consumer;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicLong;

@Component
public class SensorConsumerRunner {

    private static final Logger log = LoggerFactory.getLogger(SensorConsumerRunner.class);
    private static final String TOPIC = "sensor-readings";
    private static final Duration POLL_TIMEOUT = Duration.ofMillis(1000);
    private static final long BACKOFF_MS = 5000L;
    private static final long SUMMARY_INTERVAL_MS = 60000L;

    @Value("${kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Value("${kafka.schema-registry-url}")
    private String schemaRegistryUrl;

    @Value("${kafka.security-protocol}")
    private String securityProtocol;

    @Value("${kafka.ssl.keystore-location:}")
    private String sslKeystoreLocation;

    @Value("${kafka.ssl.keystore-password:}")
    private String sslKeystorePassword;

    @Value("${kafka.ssl.key-password:}")
    private String sslKeyPassword;

    @Value("${kafka.ssl.keystore-type:PKCS12}")
    private String sslKeystoreType;

    @Value("${kafka.ssl.truststore-location:}")
    private String sslTruststoreLocation;

    @Value("${kafka.ssl.truststore-password:}")
    private String sslTruststorePassword;

    @Value("${kafka.ssl.truststore-type:PKCS12}")
    private String sslTruststoreType;

    private final AtomicLong consumedWindow = new AtomicLong();
    private final AtomicLong failedWindow = new AtomicLong();
    // Freshness is based on poll() returning (not record count) so an idle topic doesn't flip DOWN.
    private volatile long lastSuccessNanos = System.nanoTime();
    private volatile boolean running = true;
    private Thread worker;
    private KafkaConsumer<String, GenericRecord> consumer;

    @PostConstruct
    void start() {
        consumer = new KafkaConsumer<>(buildProps());
        consumer.subscribe(List.of(TOPIC));
        worker = new Thread(this::loop, "sensor-consumer");
        worker.setDaemon(true);
        worker.start();
        log.info("Synthetic consumer started. Listening on topic '{}'.", TOPIC);
    }

    @PreDestroy
    void stop() {
        log.info("Shutting down synthetic consumer...");
        running = false;
        if (consumer != null) {
            consumer.wakeup();
        }
        if (worker != null) {
            try {
                worker.join(Duration.ofSeconds(5).toMillis());
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
            }
        }
        log.info("Synthetic consumer stopped.");
    }

    public long millisSinceLastSuccess() {
        return Duration.ofNanos(System.nanoTime() - lastSuccessNanos).toMillis();
    }

    private Properties buildProps() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "sensor-consumer-group");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class);
        props.put(KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);

        if (schemaRegistryUrl.startsWith("https://")) {
            props.put("schema.registry.ssl.keystore.location", sslKeystoreLocation);
            props.put("schema.registry.ssl.keystore.password", sslKeystorePassword);
            props.put("schema.registry.ssl.key.password", sslKeyPassword);
            props.put("schema.registry.ssl.keystore.type", sslKeystoreType);
            props.put("schema.registry.ssl.truststore.location", sslTruststoreLocation);
            props.put("schema.registry.ssl.truststore.password", sslTruststorePassword);
            props.put("schema.registry.ssl.truststore.type", sslTruststoreType);
        }
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        props.put("security.protocol", securityProtocol);
        if ("SSL".equals(securityProtocol)) {
            props.put("ssl.keystore.location", sslKeystoreLocation);
            props.put("ssl.keystore.password", sslKeystorePassword);
            props.put("ssl.key.password", sslKeyPassword);
            props.put("ssl.keystore.type", sslKeystoreType);
            props.put("ssl.truststore.location", sslTruststoreLocation);
            props.put("ssl.truststore.password", sslTruststorePassword);
            props.put("ssl.truststore.type", sslTruststoreType);
        }
        return props;
    }

    private void loop() {
        long nextSummary = System.currentTimeMillis() + SUMMARY_INTERVAL_MS;
        try {
            while (running) {
                try {
                    ConsumerRecords<String, GenericRecord> records = consumer.poll(POLL_TIMEOUT);
                    lastSuccessNanos = System.nanoTime();
                    records.forEach(record -> {
                        GenericRecord value = record.value();
                        consumedWindow.incrementAndGet();
                        log.info("Consumed: partition={}, offset={}, key={}, sensorId={}, location={}, temp={}, humidity={}, ts={}",
                                record.partition(), record.offset(), record.key(),
                                value.get("sensorId"), value.get("location"),
                                value.get("temperature"), value.get("humidity"),
                                value.get("timestamp"));
                    });

                    if (System.currentTimeMillis() >= nextSummary) {
                        log.info("window: consumed={} failed={}", consumedWindow.getAndSet(0), failedWindow.getAndSet(0));
                        nextSummary = System.currentTimeMillis() + SUMMARY_INTERVAL_MS;
                    }
                } catch (WakeupException we) {
                    if (!running) {
                        break;
                    }
                    throw we;
                } catch (Exception e) {
                    failedWindow.incrementAndGet();
                    log.warn("poll iteration failed, backing off {}ms", BACKOFF_MS, e);
                    sleepQuiet(BACKOFF_MS);
                }
            }
        } finally {
            try {
                consumer.close(Duration.ofSeconds(5));
            } catch (Exception e) {
                log.warn("consumer close failed", e);
            }
        }
    }

    private static void sleepQuiet(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
        }
    }
}

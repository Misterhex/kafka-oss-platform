package com.example.producer;

import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.time.Instant;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

@Component
public class SensorProducerRunner {

    private static final Logger log = LoggerFactory.getLogger(SensorProducerRunner.class);

    private static final String SCHEMA_JSON = """
            {
              "type": "record",
              "name": "SensorReading",
              "namespace": "com.example.avro",
              "fields": [
                {"name": "sensorId", "type": "string"},
                {"name": "location", "type": "string"},
                {"name": "temperature", "type": "double"},
                {"name": "humidity", "type": "double"},
                {"name": "timestamp", "type": "long"},
                {"name": "batteryLevel", "type": "double", "default": 100.0}
              ]
            }
            """;

    private static final String TOPIC = "sensor-readings";
    private static final String[] SENSOR_IDS = {"sensor-1", "sensor-2", "sensor-3"};
    private static final String[] LOCATIONS = {"warehouse-A", "warehouse-B", "office-floor-1"};
    private static final long SEND_INTERVAL_MS = 3000L;
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

    private final AtomicLong producedWindow = new AtomicLong();
    private final AtomicLong failedWindow = new AtomicLong();
    private volatile long lastSuccessNanos = System.nanoTime();
    private volatile boolean running = true;
    private Thread worker;
    private KafkaProducer<String, GenericRecord> producer;
    private Schema schema;

    @PostConstruct
    void start() {
        schema = new Schema.Parser().parse(SCHEMA_JSON);
        producer = new KafkaProducer<>(buildProps());
        worker = new Thread(this::loop, "sensor-producer");
        worker.setDaemon(true);
        worker.start();
        log.info("Synthetic producer started. Sending to topic '{}' every {}ms.", TOPIC, SEND_INTERVAL_MS);
    }

    @PreDestroy
    void stop() {
        log.info("Shutting down synthetic producer...");
        running = false;
        if (worker != null) {
            worker.interrupt();
        }
        if (producer != null) {
            try {
                producer.flush();
            } catch (Exception e) {
                log.warn("flush failed during shutdown", e);
            }
            producer.close(Duration.ofSeconds(5));
        }
        log.info("Synthetic producer stopped.");
    }

    public long millisSinceLastSuccess() {
        return Duration.ofNanos(System.nanoTime() - lastSuccessNanos).toMillis();
    }

    private Properties buildProps() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
        props.put(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);

        if (schemaRegistryUrl.startsWith("https://")) {
            props.put("schema.registry.ssl.keystore.location", sslKeystoreLocation);
            props.put("schema.registry.ssl.keystore.password", sslKeystorePassword);
            props.put("schema.registry.ssl.key.password", sslKeyPassword);
            props.put("schema.registry.ssl.keystore.type", sslKeystoreType);
            props.put("schema.registry.ssl.truststore.location", sslTruststoreLocation);
            props.put("schema.registry.ssl.truststore.password", sslTruststorePassword);
            props.put("schema.registry.ssl.truststore.type", sslTruststoreType);
        }

        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        props.put(ProducerConfig.ACKS_CONFIG, "all");

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
        Random random = new Random();
        long nextSummary = System.currentTimeMillis() + SUMMARY_INTERVAL_MS;

        while (running && !Thread.currentThread().isInterrupted()) {
            try {
                int idx = random.nextInt(SENSOR_IDS.length);

                GenericRecord record = new GenericData.Record(schema);
                record.put("sensorId", SENSOR_IDS[idx]);
                record.put("location", LOCATIONS[idx]);
                record.put("temperature", 18.0 + random.nextDouble() * 15.0);
                record.put("humidity", 30.0 + random.nextDouble() * 50.0);
                record.put("timestamp", Instant.now().toEpochMilli());
                record.put("batteryLevel", 50.0 + random.nextDouble() * 50.0);

                String key = SENSOR_IDS[idx];

                producer.send(new ProducerRecord<>(TOPIC, key, record), (metadata, ex) -> {
                    if (ex != null) {
                        failedWindow.incrementAndGet();
                        log.error("Failed to send record key={}", key, ex);
                    } else {
                        lastSuccessNanos = System.nanoTime();
                        producedWindow.incrementAndGet();
                        log.info("Sent: key={}, partition={}, offset={}", key, metadata.partition(), metadata.offset());
                    }
                });

                if (System.currentTimeMillis() >= nextSummary) {
                    log.info("window: produced={} failed={}", producedWindow.getAndSet(0), failedWindow.getAndSet(0));
                    nextSummary = System.currentTimeMillis() + SUMMARY_INTERVAL_MS;
                }

                Thread.sleep(SEND_INTERVAL_MS);
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                break;
            } catch (Exception e) {
                failedWindow.incrementAndGet();
                log.warn("iteration failed, backing off {}ms", BACKOFF_MS, e);
                sleepQuiet(BACKOFF_MS);
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

package com.example.producer;

import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
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
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.Properties;
import java.util.Random;

@Component
public class SensorProducerRunner implements ApplicationRunner {

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

    @Override
    public void run(ApplicationArguments args) throws Exception {
        Schema schema = new Schema.Parser().parse(SCHEMA_JSON);
        Random random = new Random();

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

        // Idempotent producer — ensures exactly-once delivery semantics
        // In Kafka 3.x+ this is the default, but setting explicitly for clarity.
        // enable.idempotence=true implies acks=all, retries=Integer.MAX_VALUE, max.in.flight.requests.per.connection<=5
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

        try (KafkaProducer<String, GenericRecord> producer = new KafkaProducer<>(props)) {
            log.info("Producer started. Sending to topic '{}' every 3 seconds...", TOPIC);

            while (!Thread.currentThread().isInterrupted()) {
                int idx = random.nextInt(SENSOR_IDS.length);

                GenericRecord record = new GenericData.Record(schema);
                record.put("sensorId", SENSOR_IDS[idx]);
                record.put("location", LOCATIONS[idx]);
                record.put("temperature", 18.0 + random.nextDouble() * 15.0);
                record.put("humidity", 30.0 + random.nextDouble() * 50.0);
                record.put("timestamp", Instant.now().toEpochMilli());
                record.put("batteryLevel", 50.0 + random.nextDouble() * 50.0);

                // Use sensorId as key — ensures all readings for the same sensor go to the same partition
                String key = SENSOR_IDS[idx];

                producer.send(new ProducerRecord<>(TOPIC, key, record), (metadata, ex) -> {
                    if (ex != null) {
                        log.error("Failed to send record", ex);
                    } else {
                        log.info("Sent: key={}, partition={}, offset={}, value={}",
                                key, metadata.partition(), metadata.offset(), record);
                    }
                });

                Thread.sleep(3000);
            }
        }
    }
}

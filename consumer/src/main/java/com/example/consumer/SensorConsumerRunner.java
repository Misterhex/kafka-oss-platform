package com.example.consumer;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

@Component
public class SensorConsumerRunner implements ApplicationRunner {

    private static final Logger log = LoggerFactory.getLogger(SensorConsumerRunner.class);
    private static final String TOPIC = "sensor-readings";

    @Value("${kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Value("${kafka.schema-registry-url}")
    private String schemaRegistryUrl;

    @Value("${kafka.schema-registry-user-info:}")
    private String schemaRegistryUserInfo;

    @Override
    public void run(ApplicationArguments args) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "sensor-consumer-group");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class);
        props.put(KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);
        props.put("basic.auth.credentials.source", "USER_INFO");
        props.put("basic.auth.user.info", schemaRegistryUserInfo);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        try (KafkaConsumer<String, GenericRecord> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(List.of(TOPIC));
            log.info("Consumer started. Listening on topic '{}'...", TOPIC);

            while (!Thread.currentThread().isInterrupted()) {
                ConsumerRecords<String, GenericRecord> records = consumer.poll(Duration.ofMillis(1000));
                records.forEach(record -> {
                    GenericRecord value = record.value();
                    log.info("Consumed: partition={}, offset={}, key={}, sensorId={}, location={}, temp={}, humidity={}, ts={}",
                            record.partition(), record.offset(), record.key(),
                            value.get("sensorId"), value.get("location"),
                            value.get("temperature"), value.get("humidity"),
                            value.get("timestamp"));
                });
            }
        }
    }
}

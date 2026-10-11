package lab;

import jakarta.annotation.PreDestroy;
import java.nio.file.*;
import java.time.Duration;
import java.util.*;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.*;
import org.springframework.boot.*;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.web.bind.annotation.*;

@SpringBootApplication
@RestController
public class SyntheticApplication {
  private final String role = System.getenv().getOrDefault("ROLE", "producer");
  private final String bootstrap = System.getenv().getOrDefault("BOOTSTRAP", "k0x:9092");
  private volatile boolean enabled = false, stopped = false, idle = true;
  private volatile long count = 0, errors = 0;
  private volatile String lastId = "", lastError = "", clusterId = "";
  private Thread worker;
  private final Path sequenceFile =
      Path.of(System.getenv().getOrDefault("SEQUENCE_FILE", "/state/sequence.json"));
  private volatile long sequence = 0;
  private final String registryUrl = RegistryClient.forBootstrap(bootstrap);
  private volatile Map<String, Integer> registeredSchemas = Map.of();

  public static void main(String[] args) {
    SpringApplication.run(SyntheticApplication.class, args);
  }

  @org.springframework.context.event.EventListener(
      org.springframework.boot.context.event.ApplicationReadyEvent.class)
  public void start() throws Exception {
    if (role.equals("producer") && Files.exists(sequenceFile)) {
      sequence = DurableJson.JSON.readValue(sequenceFile.toFile(), Long.class);
    }
    worker = Thread.ofPlatform().name(role).start(this::run);
  }

  private void run() {
    while (!stopped) {
      if (!enabled) {
        idle = true;
        sleep(100);
        continue;
      }
      idle = false;
      try {
        try (var admin = KafkaSupport.admin(bootstrap)) {
          clusterId =
              admin.describeCluster().clusterId().get(10, java.util.concurrent.TimeUnit.SECONDS);
        }
        if (role.equals("producer")) produce();
        else consume();
      } catch (Exception e) {
        errors++;
        lastError = e.toString();
        sleep(500);
      } finally {
        idle = true;
      }
    }
  }

  private void produce() throws Exception {
    var codec = new EventCodec(registryUrl);
    codec.registerFive();
    registeredSchemas = codec.registered();
    var p = KafkaSupport.properties(bootstrap);
    p.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    p.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
    p.put("acks", "all");
    p.put("enable.idempotence", "true");
    p.put("max.block.ms", "5000");
    p.put("delivery.timeout.ms", "10000");
    p.put("linger.ms", "0");
    try (var producer = new KafkaProducer<String, byte[]>(p)) {
      while (enabled && !stopped) {
        String id = UUID.randomUUID().toString();
        // Reserve durably before send: ambiguous delivery may leave gaps, never reuse numbers.
        long nextSequence = Math.incrementExact(sequence);
        DurableJson.write(sequenceFile, nextSequence);
        sequence = nextSequence;
        int partition = (int) (count % KafkaSupport.PARTITIONS);
        producer
            .send(
                new ProducerRecord<>(
                    KafkaSupport.TOPIC,
                    partition,
                    id,
                    codec.encode(id, nextSequence, java.time.Instant.now().toString())))
            .get();
        lastId = id;
        count++;
        sleep(100);
      }
    }
  }

  private void consume() throws Exception {
    var codec = new EventCodec(registryUrl);
    var p = KafkaSupport.properties(bootstrap);
    p.put("group.id", KafkaSupport.GROUP);
    p.put("enable.auto.commit", "false");
    p.put("auto.offset.reset", "earliest");
    p.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    p.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
    p.put("max.poll.records", "100");
    var consumer = new KafkaConsumer<String, byte[]>(p);
    try {
      consumer.subscribe(List.of(KafkaSupport.TOPIC));
      while (enabled && !stopped) {
        var records = consumer.poll(Duration.ofMillis(300));
        for (var r : records) {
          var event = codec.decode(r.value());
          if (!r.key().equals(event.path("id").asText()))
            throw new IllegalStateException("Event ID does not match Kafka key");
          count++;
          lastId = r.key();
        }
        if (!records.isEmpty()) consumer.commitSync();
      }
    } finally {
      consumer.close(Duration.ofSeconds(5));
    }
  }

  @GetMapping("/status")
  public Map<String, Object> status() {
    var status = new LinkedHashMap<String, Object>(Map.of(
        "role",
        role,
        "enabled",
        enabled,
        "idle",
        idle,
        "count",
        count,
        "errors",
        errors,
        "lastId",
        lastId,
        "lastError",
        lastError,
        "seqNo",
        sequence,
        "clusterId",
        clusterId,
        "bootstrap",
        bootstrap));
    status.put("schemaRegistryUrl", registryUrl);
    status.put("registeredSchemas", registeredSchemas);
    return status;
  }

  @PostMapping("/pause")
  public synchronized Map<String, Object> pause() {
    enabled = false;
    long deadline = System.nanoTime() + Duration.ofSeconds(40).toNanos();
    while (!idle) {
      if (System.nanoTime() > deadline) throw new IllegalStateException("Client did not close");
      sleep(50);
    }
    return status();
  }

  @PostMapping("/resume")
  public synchronized Map<String, Object> resume() {
    enabled = true;
    return status();
  }

  @PreDestroy
  public void stop() throws InterruptedException {
    stopped = true;
    enabled = false;
    if (worker != null) worker.join(20000);
  }

  private static void sleep(long ms) {
    try {
      Thread.sleep(ms);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }
}

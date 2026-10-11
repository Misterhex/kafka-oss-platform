package lab.schema;

import com.fasterxml.jackson.databind.JsonNode;
import java.nio.file.*;
import java.nio.file.attribute.PosixFilePermissions;
import java.security.MessageDigest;
import java.time.*;
import java.util.*;
import java.util.concurrent.TimeUnit;
import lab.*;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.header.internals.RecordHeaders;

/** Byte-exact commit-log backup preserves registry state without allocating new schema IDs. */
public final class SchemaLogBackup {
  public record Header(String name, String value) {}
  public record Entry(long sourceOffset, long timestamp, String key, String value, List<Header> headers) {}
  public record Snapshot(int formatVersion, String createdAt, String sourceClusterId, String sourceTopic,
                         long beginningOffset, long endOffset, List<Entry> records) {}
  private final String bootstrap, topic;
  private final Properties properties;
  private static final Base64.Encoder ENCODER = Base64.getEncoder();
  private static final Base64.Decoder DECODER = Base64.getDecoder();

  public SchemaLogBackup(String bootstrap, String topic, Properties supplied) {
    this.bootstrap = bootstrap; this.topic = topic;
    properties = KafkaSupport.properties(bootstrap);
    properties.putAll(supplied);
    properties.put("bootstrap.servers", bootstrap);
  }

  private KafkaConsumer<byte[], byte[]> reader() {
    var p = new Properties(); p.putAll(properties);
    p.remove("group.id");
    p.put("key.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
    p.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
    p.put("enable.auto.commit", "false");
    p.put("isolation.level", "read_committed");
    return new KafkaConsumer<>(p);
  }

  private void validateTopic(Admin admin) throws Exception {
    var description = admin.describeTopics(List.of(topic)).allTopicNames().get(20, TimeUnit.SECONDS).get(topic);
    if (description.partitions().size() != 1) throw new IllegalStateException("Schema Registry commit log must have exactly one partition");
    var resource = new ConfigResource(ConfigResource.Type.TOPIC, topic);
    String policy = admin.describeConfigs(List.of(resource)).all().get(20, TimeUnit.SECONDS).get(resource).get("cleanup.policy").value();
    if (!policy.equals("compact")) throw new IllegalStateException("Expected cleanup.policy=compact; found " + policy);
  }

  public Snapshot capture() throws Exception {
    try (var admin = Admin.create(properties); var consumer = reader()) {
      validateTopic(admin);
      var tp = new TopicPartition(topic, 0);
      consumer.assign(List.of(tp));
      long beginning = consumer.beginningOffsets(List.of(tp)).get(tp);
      long end = consumer.endOffsets(List.of(tp)).get(tp);
      consumer.seek(tp, beginning);
      var records = new ArrayList<Entry>();
      long deadline = System.nanoTime() + Duration.ofMinutes(3).toNanos();
      while (consumer.position(tp) < end) {
        if (System.nanoTime() > deadline) throw new IllegalStateException("Backup scan timed out");
        for (var record : consumer.poll(Duration.ofMillis(250))) {
          if (record.offset() >= end) continue;
          var headers = new ArrayList<Header>();
          record.headers().forEach(h -> headers.add(new Header(h.key(), encode(h.value()))));
          records.add(new Entry(record.offset(), record.timestamp(), encode(record.key()), encode(record.value()), headers));
        }
      }
      long finalEnd = consumer.endOffsets(List.of(tp)).get(tp);
      if (finalEnd != end) throw new IllegalStateException("Registry changed during backup; quiesce schema/config/mode/deletion writes and retry");
      return new Snapshot(1, Instant.now().toString(), admin.describeCluster().clusterId().get(15, TimeUnit.SECONDS), topic, beginning, end, records);
    }
  }

  public void backup(Path file, boolean overwrite) throws Exception {
    if (Files.exists(file) && !overwrite) throw new IllegalArgumentException("Backup already exists; use another file or --overwrite");
    Snapshot snapshot = capture();
    var payload = DurableJson.JSON.valueToTree(snapshot);
    var envelope = DurableJson.JSON.createObjectNode();
    envelope.put("format", "confluent-schema-registry-kafka-log-v1");
    envelope.put("sha256", digest(payload));
    envelope.set("snapshot", payload);
    Files.createDirectories(file.getParent());
    Path temp = file.resolveSibling(file.getFileName() + ".tmp");
    Files.deleteIfExists(temp);
    Files.createFile(temp);
    try { Files.setPosixFilePermissions(temp, PosixFilePermissions.fromString("rw-------")); }
    catch (UnsupportedOperationException ignored) { }
    DurableJson.write(file, envelope);
    System.out.println("BACKUP OK: " + snapshot.records().size() + " log records, source boundary " + snapshot.endOffset() + ", file " + file);
  }

  public static Snapshot load(Path file) throws Exception {
    return load(file, false);
  }

  public static Snapshot load(Path file, boolean allowChecksumMismatch) throws Exception {
    JsonNode envelope = DurableJson.JSON.readTree(file.toFile());
    if (!envelope.path("format").asText().equals("confluent-schema-registry-kafka-log-v1"))
      throw new IllegalArgumentException("Unsupported backup format");
    JsonNode payload = envelope.path("snapshot");
    if (!digest(payload).equals(envelope.path("sha256").asText())) {
      if (!allowChecksumMismatch) throw new IllegalArgumentException("Backup checksum mismatch");
      System.err.println("WARNING: backup checksum mismatch accepted via --allow-checksum-mismatch: " + file + "; file integrity is unverified. Remaining validation still applies.");
    }
    Snapshot snapshot = DurableJson.JSON.treeToValue(payload, Snapshot.class);
    validate(snapshot);
    return snapshot;
  }

  static void validate(Snapshot snapshot) {
    if (snapshot.formatVersion() != 1 || snapshot.beginningOffset() < 0 || snapshot.endOffset() < snapshot.beginningOffset())
      throw new IllegalArgumentException("Invalid snapshot boundaries/version");
    long previous = snapshot.beginningOffset() - 1;
    if (snapshot.records() == null) throw new IllegalArgumentException("Missing records");
    for (Entry entry : snapshot.records()) {
      if (entry.key() == null || entry.headers() == null)
        throw new IllegalArgumentException("Missing registry key/headers");
      if (entry.sourceOffset() <= previous || entry.sourceOffset() >= snapshot.endOffset())
        throw new IllegalArgumentException("Unordered or out-of-bound record");
      decode(entry.key()); decode(entry.value());
      entry.headers().forEach(h -> decode(h.value()));
      previous = entry.sourceOffset();
    }
  }

  public void restore(Path file, short replicationFactor) throws Exception {
    restore(file, replicationFactor, false);
  }

  public void restore(Path file, short replicationFactor, boolean allowChecksumMismatch) throws Exception {
    Snapshot snapshot = load(file, allowChecksumMismatch); // Validate input before any Kafka mutation.
    if (replicationFactor < 1) throw new IllegalArgumentException("replication-factor must be positive");
    try (var admin = Admin.create(properties)) {
      if (!admin.listTopics().names().get(20, TimeUnit.SECONDS).contains(topic))
        admin.createTopics(List.of(new NewTopic(topic, 1, replicationFactor)
            .configs(Map.of("cleanup.policy", "compact", "min.insync.replicas", "1"))))
            .all().get(20, TimeUnit.SECONDS);
      validateTopic(admin);
      try (var consumer = reader()) {
        var tp = new TopicPartition(topic, 0);
        consumer.assign(List.of(tp));
        long start = consumer.beginningOffsets(List.of(tp)).get(tp), end = consumer.endOffsets(List.of(tp)).get(tp);
        if (start != 0 || end != 0) throw new IllegalStateException("Restore target must be EMPTY and never used: " + topic + " has offsets " + start + ".." + end);
      }
    }
    var p = new Properties(); p.putAll(properties);
    p.put("key.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
    p.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
    p.put("acks", "all"); p.put("enable.idempotence", "true"); p.put("max.in.flight.requests.per.connection", "1");
    try (var producer = new KafkaProducer<byte[], byte[]>(p)) {
      long expectedOffset = 0;
      for (Entry entry : snapshot.records()) {
        var headers = new RecordHeaders();
        entry.headers().forEach(h -> headers.add(h.name(), decode(h.value())));
        var record = new ProducerRecord<byte[], byte[]>(topic, 0, entry.timestamp() >= 0 ? entry.timestamp() : null,
            decode(entry.key()), decode(entry.value()), headers);
        long actual = producer.send(record).get(30, TimeUnit.SECONDS).offset();
        if (actual != expectedOffset++) throw new IllegalStateException("Concurrent destination writer detected; keep registry stopped and retry into a new topic");
      }
      producer.flush();
    }
    Snapshot restored = capture();
    if (!logicalState(snapshot).equals(logicalState(restored)))
      throw new IllegalStateException("Restored commit-log state differs from backup");
    System.out.println("RESTORE OK: " + snapshot.records().size() + " records to " + bootstrap + "/" + topic + "; logical state verified. Start destination Schema Registry now.");
  }

  /** Compaction may remove historical entries; compare the final value for every key. */
  static Map<String, String> logicalState(Snapshot snapshot) {
    Map<String, String> state = new TreeMap<>();
    for (Entry entry : snapshot.records()) {
      if (entry.key() == null) throw new IllegalArgumentException("Registry record has null key");
      if (entry.value() == null) state.remove(entry.key()); else state.put(entry.key(), entry.value());
    }
    return state;
  }

  public static void inspect(Path file) throws Exception {
    Snapshot snapshot = load(file);
    System.out.println("Source: " + snapshot.sourceClusterId() + "/" + snapshot.sourceTopic() + "; records: " + snapshot.records().size());
    for (var entry : logicalState(snapshot).entrySet()) {
      var output = DurableJson.JSON.createObjectNode();
      output.set("key", jsonOrBase64(entry.getKey())); output.set("value", jsonOrBase64(entry.getValue()));
      System.out.println(DurableJson.JSON.writeValueAsString(output));
    }
  }

  private static JsonNode jsonOrBase64(String value) {
    try { return DurableJson.JSON.readTree(decode(value)); }
    catch (Exception e) { return DurableJson.JSON.getNodeFactory().textNode(value); }
  }
  static String digest(JsonNode payload) throws Exception {
    return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(DurableJson.JSON.writeValueAsBytes(payload)));
  }
  static String encode(byte[] value) { return value == null ? null : ENCODER.encodeToString(value); }
  static byte[] decode(String value) { return value == null ? null : DECODER.decode(value); }
}

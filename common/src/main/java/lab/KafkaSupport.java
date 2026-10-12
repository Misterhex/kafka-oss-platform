package lab;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;

public final class KafkaSupport {
  public static final String TOPIC = "appTopic", GROUP = "dr-consumer";
  public static final int PARTITIONS = 1;

  public static Properties properties(String bootstrap) {
    Properties p = new Properties();
    p.put("bootstrap.servers", bootstrap);
    if (System.getenv().getOrDefault("LAB_KAFKA_SECURITY_PROTOCOL", "PLAINTEXT").equals("SASL_PLAINTEXT")) {
      p.put("security.protocol", "SASL_PLAINTEXT");
      p.put("sasl.mechanism", "PLAIN");
      String user = System.getenv().getOrDefault("LAB_KAFKA_USERNAME", "platform");
      String password = System.getenv().getOrDefault("LAB_KAFKA_PASSWORD", "platform-lab");
      p.put("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username=\""
          + user + "\" password=\"" + password + "\";");
    }
    p.put("request.timeout.ms", "5000");
    p.put("default.api.timeout.ms", "15000");
    p.put("client.dns.lookup", "use_all_dns_ips");
    return p;
  }

  public static List<TopicPartition> partitions() {
    return java.util.stream.IntStream.range(0, PARTITIONS)
        .mapToObj(i -> new TopicPartition(TOPIC, i))
        .toList();
  }

  public static KafkaConsumer<String, byte[]> reader(String bootstrap) {
    Properties p = properties(bootstrap);
    p.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    p.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
    p.put("enable.auto.commit", "false");
    return new KafkaConsumer<>(p);
  }

  public static Admin admin(String bootstrap) {
    return Admin.create(properties(bootstrap));
  }

  public static void ensureTopic(String bootstrap) throws Exception {
    try (Admin a = admin(bootstrap)) {
      try {
        a.createTopics(
                List.of(
                    new NewTopic(TOPIC, PARTITIONS, (short) 1)
                        .configs(Map.of("min.insync.replicas", "1", "retention.ms", "604800000"))))
            .all()
            .get(20, TimeUnit.SECONDS);
      } catch (java.util.concurrent.ExecutionException e) {
        if (!(e.getCause() instanceof org.apache.kafka.common.errors.TopicExistsException)) throw e;
      }
    }
  }

  // Bounded full scan is intentional: this is a small synthetic lab, not a production offset index.
  public static Map<Integer, LinkedHashMap<String, Long>> scan(String bootstrap) {
    try (var c = reader(bootstrap)) {
      var ps = partitions();
      c.assign(ps);
      c.seekToBeginning(ps);
      var ends = c.endOffsets(ps);
      Map<Integer, LinkedHashMap<String, Long>> result = new TreeMap<>();
      ps.forEach(p -> result.put(p.partition(), new LinkedHashMap<>()));
      long deadline = System.nanoTime() + Duration.ofSeconds(20).toNanos();
      while (ps.stream().anyMatch(p -> c.position(p) < ends.get(p))) {
        if (System.nanoTime() > deadline)
          throw new IllegalStateException("Timed out scanning " + bootstrap);
        for (var r : c.poll(Duration.ofMillis(200)))
          if (r.offset() < ends.get(new TopicPartition(r.topic(), r.partition())))
            result.get(r.partition()).putIfAbsent(r.key(), r.offset());
      }
      return result;
    }
  }

  private KafkaSupport() {}
}

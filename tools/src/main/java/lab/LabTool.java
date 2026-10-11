package lab;

import com.fasterxml.jackson.databind.JsonNode;
import java.net.URI;
import java.net.http.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.common.TopicPartition;

/** Small Java CLI for manual Connect operations and integration assertions. */
public final class LabTool {
  private static final String CONTROLLER = "http://dr-controller:8080";
  private static final HttpClient HTTP =
      HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();

  public static void main(String[] args) throws Exception {
    Object result =
        switch (args[0]) {
          case "wait-connector" -> waitConnector(args[1], args[2], args[3]);
          case "reverse-offsets" -> reverseOffsets(new String(System.in.readAllBytes()), args[1]);
          case "akhq" -> checkAkhq();
          case "snapshot" -> snapshot();
          case "paused" -> paused();
          case "verify" -> verify(DurableJson.JSON.readTree(System.in));
          case "same-checkpoint" -> sameCheckpoint(DurableJson.JSON.readTree(System.in));
          case "expect-phase" -> expectPhase(args[1]);
          case "wait-clients" -> await(() -> get(CONTROLLER + "/api/apps"));
          case "wait-controller" -> await(() -> get(CONTROLLER + "/api/status"));
          default ->
              throw new IllegalArgumentException(
                  "Commands: wait-connector URL NAME STATE | reverse-offsets ALIAS (stdin) | akhq |"
                      + " snapshot | verify (stdin) | wait-controller");
        };
    System.out.println(
        DurableJson.JSON.writerWithDefaultPrettyPrinter().writeValueAsString(result));
  }

  private static JsonNode expectPhase(String phase) throws Exception {
    var state = get(CONTROLLER + "/api/status");
    require(
        state.path("phase").asText().equals(phase), "Expected phase " + phase + ", got " + state);
    return state;
  }

  private static JsonNode sameCheckpoint(JsonNode saved) throws Exception {
    var state = get(CONTROLLER + "/api/status");
    require(
        state.path("checkpoint").equals(saved.path("checkpoint")),
        "Checkpoint changed after restart");
    require(
        state.path("restoredOffsets").equals(saved.path("restoredOffsets")),
        "Restored offsets changed");
    return state;
  }

  private static JsonNode get(String url) throws Exception {
    return request("GET", url);
  }

  private static JsonNode request(String method, String url) throws Exception {
    var response =
        HTTP.send(
            HttpRequest.newBuilder(URI.create(url))
                .timeout(Duration.ofSeconds(60))
                .method(method, HttpRequest.BodyPublishers.noBody())
                .build(),
            HttpResponse.BodyHandlers.ofString());
    require(response.statusCode() / 100 == 2, response.statusCode() + " " + response.body());
    return DurableJson.JSON.readTree(response.body());
  }

  private interface Attempt {
    JsonNode run() throws Exception;
  }

  private static JsonNode await(Attempt action) throws Exception {
    long deadline = System.nanoTime() + Duration.ofSeconds(120).toNanos();
    Exception last = null;
    while (System.nanoTime() < deadline) {
      try {
        return action.run();
      } catch (Exception e) {
        last = e;
        Thread.sleep(1000);
      }
    }
    throw new IllegalStateException("Timed out waiting for lab", last);
  }

  private static void require(boolean condition, String message) {
    if (!condition) throw new IllegalStateException(message);
  }

  private static JsonNode waitConnector(String base, String name, String expected)
      throws Exception {
    require(Set.of("RUNNING", "STOPPED").contains(expected), "Invalid connector state");
    return await(
        () -> {
          var status = get(base + "/connectors/" + name + "/status");
          require(
              status.path("connector").path("state").asText().equals(expected), status.toString());
          var tasks = status.path("tasks");
          if (expected.equals("STOPPED")) require(tasks.isEmpty(), "Tasks still stopping");
          else {
            require(!tasks.isEmpty(), "No running tasks");
            for (var task : tasks)
              require(task.path("state").asText().equals("RUNNING"), task.toString());
          }
          return status;
        });
  }

  static Map<String, Object> reverseOffsets(String input, String alias) {
    List<Object> offsets = new ArrayList<>();
    Set<Integer> seen = new HashSet<>();
    for (String line : input.lines().filter(l -> !l.isBlank()).toList()) {
      String[] fields = line.trim().split(":");
      require(
          fields.length == 3 && fields[0].equals(KafkaSupport.TOPIC),
          "Expected appTopic:partition:offset");
      int partition = Integer.parseInt(fields[1]);
      long next = Long.parseLong(fields[2]);
      require(
          next >= 0 && partition >= 0 && partition < KafkaSupport.PARTITIONS && seen.add(partition),
          "Invalid or duplicate partition");
      Map<String, Object> item = new LinkedHashMap<>();
      item.put("partition", Map.of("cluster", alias, "topic", fields[0], "partition", partition));
      item.put("offset", next == 0 ? null : Map.of("offset", next - 1));
      offsets.add(item);
    }
    require(seen.size() == KafkaSupport.PARTITIONS, "Record every appTopic partition offset");
    return Map.of("offsets", offsets);
  }

  private static JsonNode checkAkhq() throws Exception {
    var clusters = get("http://akhq:8080/api/cluster");
    require(clusters.size() == 2, "Expected two AKHQ clusters");
    for (String cluster : List.of("k0x-hk", "k0y-sg")) {
      String worker = cluster.startsWith("k0x") ? "connect-k0x" : "connect-k0y";
      require(
          get("http://akhq:8080/api/" + cluster + "/topic/name")
              .toString()
              .contains(KafkaSupport.TOPIC),
          "Topic missing from AKHQ");
      require(
          get("http://akhq:8080/api/" + cluster + "/connect/" + worker + "/plugins")
              .toString()
              .contains("MirrorSourceConnector"),
          "Worker missing from AKHQ");
    }
    return clusters;
  }

  private static JsonNode traffic(String cluster) throws Exception {
    String id;
    try (var admin = KafkaSupport.admin(cluster + ":19092")) {
      id = admin.describeCluster().clusterId().get(15, TimeUnit.SECONDS);
    }
    final String expectedId = id;
    return await(
        () -> {
          var apps = get(CONTROLLER + "/api/apps");
          for (String role : List.of("producer", "consumer")) {
            var app = apps.path(role);
            require(
                app.path("bootstrap").asText().equals(cluster + ":9092"), "Wrong direct bootstrap");
            require(app.path("clusterId").asText().equals(expectedId), "Wrong connected cluster");
            require(app.path("count").asLong() >= 20, "Waiting for traffic");
          }
          return apps;
        });
  }

  private static Object snapshot() throws Exception {
    var before = traffic("k0x");
    await(LabTool::checkAkhq);
    var clusters = get(CONTROLLER + "/api/clusters");
    require(!clusters.path("k0x").equals(clusters.path("k0y")), "Clusters must be independent");
    var checkpoint = request("POST", CONTROLLER + "/api/checkpoint");
    require(
        checkpoint.path("checkpoint").path("committedIds").size() == KafkaSupport.PARTITIONS,
        "Missing partition checkpoints");
    var source = get(CONTROLLER + "/api/records?cluster=k0x");
    await(
        () -> {
          var target = get(CONTROLLER + "/api/records?cluster=k0y");
          contains(source, target);
          return target;
        });
    return Map.of("before", before, "source", source, "clusters", clusters);
  }

  private static void contains(JsonNode source, JsonNode target) {
    source
        .fields()
        .forEachRemaining(
            p ->
                p.getValue()
                    .fieldNames()
                    .forEachRemaining(
                        id ->
                            require(
                                target.path(p.getKey()).has(id),
                                "Missing replicated event " + id)));
  }

  private static JsonNode paused() throws Exception {
    var state = expectPhase("SG_ACTIVE");
    var apps = get(CONTROLLER + "/api/apps");
    for (var app : apps) {
      require(
          !app.path("enabled").asBoolean() && app.path("idle").asBoolean(),
          "Clients must remain paused after failover");
      require(
          app.path("bootstrap").asText().equals("k0x:9092"),
          "Controller must not retarget clients");
    }
    waitConnector("http://connect-k0y:8083", "hk-to-sg", "STOPPED");
    return state;
  }

  private static Object verify(JsonNode snapshot) throws Exception {
    var apps = traffic("k0y");
    require(
        apps.path("producer").path("seqNo").asLong()
            > snapshot.path("before").path("producer").path("seqNo").asLong(),
        "Sequence reset after bounce");
    var state = get(CONTROLLER + "/api/status");
    require(state.path("phase").asText().equals("SG_ACTIVE"), "SG not active");
    var target = get(CONTROLLER + "/api/records?cluster=k0y");
    contains(snapshot.path("source"), target);
    state
        .path("restoredOffsets")
        .fields()
        .forEachRemaining(
            entry -> {
              String id =
                  state.path("checkpoint").path("committedIds").path(entry.getKey()).asText();
              long expected =
                  target.path(entry.getKey()).has(id)
                      ? target.path(entry.getKey()).path(id).asLong() + 1
                      : 0;
              require(entry.getValue().asLong() == expected, "Incorrect offset translation");
            });
    try (var admin = KafkaSupport.admin("k0y:19092")) {
      var offsets =
          admin
              .listConsumerGroupOffsets(KafkaSupport.GROUP)
              .partitionsToOffsetAndMetadata()
              .get(15, TimeUnit.SECONDS);
      for (int p = 0; p < KafkaSupport.PARTITIONS; p++) {
        require(
            offsets.get(new TopicPartition(KafkaSupport.TOPIC, p)).offset()
                > state.path("restoredOffsets").path("" + p).asLong(),
            "Consumer commit did not advance");
      }
    }
    List<JsonNode> payloads = new ArrayList<>();
    var codec = new EventCodec(RegistryClient.forBootstrap("k0y:19092"));
    try (var consumer = KafkaSupport.reader("k0y:19092")) {
      consumer.assign(KafkaSupport.partitions());
      consumer.seekToBeginning(KafkaSupport.partitions());
      long deadline = System.nanoTime() + Duration.ofSeconds(15).toNanos();
      while (payloads.size() < 6 && System.nanoTime() < deadline) {
        for (var record : consumer.poll(Duration.ofMillis(250))) {
          var payload = codec.decode(record.value());
          require(
              payload.path("seqNo").asLong() > 0
                  && payload.path("id").asText().equals(record.key()),
              "Invalid event payload");
          if (payloads.size() < 6) payloads.add(payload);
        }
      }
    }
    require(payloads.size() == 6, "No payload samples");
    await(LabTool::checkAkhq);
    return Map.of("result", "PASS", "state", state, "apps", apps, "samplePayloads", payloads);
  }

  private LabTool() {}
}

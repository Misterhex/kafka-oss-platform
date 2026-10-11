package lab;

import java.net.URI;
import java.net.http.*;
import java.nio.file.*;
import java.time.*;
import java.util.*;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.springframework.boot.*;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.scheduling.annotation.*;
import org.springframework.web.bind.annotation.*;

@SpringBootApplication
@EnableScheduling
@RestController
public class DrApplication {
  private static final String HK = "k0x:19092",
      SG = "k0y:19092",
      CONNECT = "http://connect-k0y:8083/connectors/hk-to-sg";
  private final Path statePath =
      Path.of(System.getenv().getOrDefault("STATE_DIR", "/state"), "state.json");
  private final HttpClient http =
      HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();

  public static class State {
    public String phase = "NEW", primary = "k0x", error = "";
    public Checkpoint checkpoint = new Checkpoint();
    public Map<Integer, Long> restoredOffsets = new TreeMap<>();
    public boolean forced = false, checkpointFrozen = false;
  }

  private State state;

  public DrApplication() throws Exception {
    state =
        Files.exists(statePath)
            ? DurableJson.JSON.readValue(statePath.toFile(), State.class)
            : new State();
  }

  public static void main(String[] args) {
    SpringApplication.run(DrApplication.class, args);
  }

  private void save() throws Exception {
    DurableJson.write(statePath, state);
  }

  private static class HttpFailure extends IllegalStateException {
    final int status;

    HttpFailure(int status, String message) {
      super(message);
      this.status = status;
    }
  }

  private String request(String method, String url, Object body) throws Exception {
    var b = HttpRequest.newBuilder(URI.create(url)).timeout(Duration.ofSeconds(55));
    b.header("Content-Type", "application/json")
        .method(
            method,
            body == null
                ? HttpRequest.BodyPublishers.noBody()
                : HttpRequest.BodyPublishers.ofString(DurableJson.JSON.writeValueAsString(body)));
    var r = http.send(b.build(), HttpResponse.BodyHandlers.ofString());
    if (r.statusCode() < 200 || r.statusCode() >= 300)
      throw new HttpFailure(
          r.statusCode(), method + " " + url + " => " + r.statusCode() + " " + r.body());
    return r.body();
  }

  private void apps(String action) throws Exception {
    request("POST", "http://synthetic-producer:8080/" + action, null);
    request("POST", "http://synthetic-consumer:8080/" + action, null);
  }

  @GetMapping("/api/status")
  public synchronized State status() {
    return state;
  }

  @GetMapping("/api/apps")
  public Map<String, Object> appStatus() throws Exception {
    return Map.of(
        "producer",
        DurableJson.JSON.readValue(
            request("GET", "http://synthetic-producer:8080/status", null), Object.class),
        "consumer",
        DurableJson.JSON.readValue(
            request("GET", "http://synthetic-consumer:8080/status", null), Object.class));
  }

  @GetMapping("/api/records")
  public Map<Integer, LinkedHashMap<String, Long>> records(@RequestParam String cluster) {
    if (!Set.of("k0x", "k0y").contains(cluster))
      throw new IllegalArgumentException("Unknown cluster");
    return KafkaSupport.scan(cluster.equals("k0x") ? HK : SG);
  }

  @GetMapping("/api/clusters")
  public Map<String, String> clusters() throws Exception {
    try (var x = KafkaSupport.admin(HK);
        var y = KafkaSupport.admin(SG)) {
      return Map.of(
          "k0x",
          x.describeCluster().clusterId().get(10, TimeUnit.SECONDS),
          "k0y",
          y.describeCluster().clusterId().get(10, TimeUnit.SECONDS));
    }
  }

  @PostMapping("/api/bootstrap")
  public synchronized State bootstrap() throws Exception {
    if (!Set.of("NEW", "HK_ACTIVE", "SG_ACTIVE").contains(state.phase))
      throw new IllegalStateException(
          "Interrupted transition: retry /api/failover with its original mode");
    if (state.phase.equals("SG_ACTIVE")) return state;
    apps("pause");
    for (String service : List.of("synthetic-producer", "synthetic-consumer")) {
      var client =
          DurableJson.JSON.readTree(request("GET", "http://" + service + ":8080/status", null));
      if (!client.path("bootstrap").asText().equals("k0x:9092"))
        throw new IllegalStateException(
            "Initial bootstrap requires clients configured for k0x:9092");
    }
    KafkaSupport.ensureTopic(HK);
    KafkaSupport.ensureTopic(SG);
    Map<String, String> config = new LinkedHashMap<>();
    config.put("connector.class", "org.apache.kafka.connect.mirror.MirrorSourceConnector");
    config.put("tasks.max", "1");
    config.put("source.cluster.alias", "hk");
    config.put("target.cluster.alias", "sg");
    config.put("source.cluster.bootstrap.servers", HK);
    config.put("target.cluster.bootstrap.servers", SG);
    config.put("topics", KafkaSupport.TOPIC);
    config.put(
        "replication.policy.class", "org.apache.kafka.connect.mirror.IdentityReplicationPolicy");
    config.put("replication.factor", "1");
    config.put("offset-syncs.topic.replication.factor", "1");
    config.put("offset-syncs.topic.location", "target");
    config.put("refresh.topics.interval.seconds", "1");
    config.put("sync.topic.acls.enabled", "false");
    config.put("sync.topic.configs.enabled", "false");
    config.put("emit.heartbeats.enabled", "false");
    config.put("key.converter", "org.apache.kafka.connect.converters.ByteArrayConverter");
    config.put("value.converter", "org.apache.kafka.connect.converters.ByteArrayConverter");
    request("PUT", CONNECT + "/config", config);
    waitConnector("RUNNING");
    state.phase = "HK_ACTIVE";
    state.primary = "k0x";

    save();
    apps("resume");
    return state;
  }

  private void waitConnector(String expected) throws Exception {
    long end = System.nanoTime() + Duration.ofSeconds(60).toNanos();
    while (System.nanoTime() < end) {
      com.fasterxml.jackson.databind.JsonNode json;
      try {
        json = DurableJson.JSON.readTree(request("GET", CONNECT + "/status", null));
      } catch (HttpFailure e) {
        // Connect writes status asynchronously after accepting a connector configuration.
        if (e.status != 404 && e.status != 409 && e.status != 503) throw e;
        Thread.sleep(500);
        continue;
      }
      if (expected.equals("RUNNING")
          && json.path("connector").path("state").asText().equals("FAILED"))
        throw new IllegalStateException("Connector failed: " + json);
      boolean ok = json.path("connector").path("state").asText().equals(expected);
      var tasks = json.path("tasks");
      if (expected.equals("RUNNING")) ok &= !tasks.isEmpty();
      for (var t : tasks) {
        if (expected.equals("RUNNING") && t.path("state").asText().equals("FAILED"))
          throw new IllegalStateException("Mirror task failed: " + t);
        ok &= t.path("state").asText().equals(expected);
      }
      if (ok) return;
      Thread.sleep(500);
    }
    throw new IllegalStateException("Connector did not become " + expected);
  }

  private Checkpoint capture() throws Exception {
    Checkpoint cp = new Checkpoint();
    cp.capturedAt = Instant.now().toString();
    try (var a = KafkaSupport.admin(HK);
        var c = KafkaSupport.reader(HK)) {
      var commits =
          a.listConsumerGroupOffsets(KafkaSupport.GROUP)
              .partitionsToOffsetAndMetadata()
              .get(15, TimeUnit.SECONDS);
      var ps = KafkaSupport.partitions();
      c.assign(ps);
      var ends = c.endOffsets(ps);
      var beginnings = c.beginningOffsets(ps);
      for (var p : ps) {
        long offset = commits.getOrDefault(p, new OffsetAndMetadata(0)).offset();
        cp.sourceOffsets.put(p.partition(), offset);
        if (offset > beginnings.get(p))
          cp.committedIds.put(p.partition(), readKey(c, p, offset - 1));
        else if (offset > 0)
          throw new IllegalStateException("Source checkpoint expired from retention");
        if (ends.get(p) > beginnings.get(p))
          cp.barriers.put(p.partition(), readKey(c, p, ends.get(p) - 1));
      }
    }
    return cp;
  }

  private String readKey(KafkaConsumer<String, byte[]> c, TopicPartition p, long offset) {
    c.assign(List.of(p));
    c.seek(p, offset);
    long deadline = System.nanoTime() + Duration.ofSeconds(10).toNanos();
    while (System.nanoTime() < deadline)
      for (var r : c.poll(Duration.ofMillis(200))) if (r.offset() == offset) return r.key();
    throw new IllegalStateException("No retained event at " + p + "/" + offset);
  }

  @Scheduled(fixedDelay = 3000)
  public synchronized void checkpoint() {
    if (!state.phase.equals("HK_ACTIVE")) return;
    try {
      state.checkpoint = capture();
      state.error = "";
      save();
    } catch (Exception e) {
      state.error = "Checkpoint: " + e;
    }
  }

  @PostMapping("/api/checkpoint")
  public synchronized State manualCheckpoint() throws Exception {
    if (!state.phase.equals("HK_ACTIVE")) throw new IllegalStateException("HK is not active");
    state.checkpoint = capture();
    save();
    return state;
  }

  @PostMapping("/api/failover")
  public synchronized State failover(@RequestParam(defaultValue = "false") boolean force)
      throws Exception {
    if (state.phase.equals("SG_ACTIVE")) return state;
    if (state.phase.equals("NEW")) throw new IllegalStateException("Bootstrap first");
    if (!state.phase.equals("HK_ACTIVE") && state.forced != force)
      throw new IllegalStateException("Retry with the original force mode");
    try {
      boolean fresh = state.phase.equals("HK_ACTIVE");
      if (fresh) state.checkpointFrozen = false;
      state.forced = force;
      state.phase = "QUIESCING";
      save();
      apps("pause");
      if (!force && !state.checkpointFrozen) {
        state.checkpoint = capture();
        state.checkpointFrozen = true;
        save();
      }
      state.phase = "DRAINING";
      save();
      Map<Integer, LinkedHashMap<String, Long>> target = KafkaSupport.scan(SG);
      if (!force) {
        long deadline = System.nanoTime() + Duration.ofSeconds(90).toNanos();
        while (!hasBarriers(target, state.checkpoint)) {
          if (System.nanoTime() > deadline)
            throw new IllegalStateException(
                "Replication drain timed out; applications remain paused");
          Thread.sleep(500);
          target = KafkaSupport.scan(SG);
        }
      }
      request("PUT", CONNECT + "/stop", null);
      waitConnector("STOPPED");
      target = KafkaSupport.scan(SG);
      state.restoredOffsets = Checkpoint.translate(state.checkpoint, target, !force);
      try (var a = KafkaSupport.admin(SG)) {
        Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
        state.restoredOffsets.forEach(
            (p, o) ->
                offsets.put(new TopicPartition(KafkaSupport.TOPIC, p), new OffsetAndMetadata(o)));
        a.alterConsumerGroupOffsets(KafkaSupport.GROUP, offsets).all().get(20, TimeUnit.SECONDS);
      }
      state.phase = "PROMOTING";
      save();
      state.primary = "k0y";
      state.phase = "SG_ACTIVE";
      state.error = "";
      save();
      return state;
    } catch (Exception e) {
      state.error = e.toString();
      save();
      throw e;
    }
  }

  static boolean hasBarriers(Map<Integer, ? extends Map<String, Long>> target, Checkpoint cp) {
    return cp.barriers.entrySet().stream()
        .allMatch(e -> target.get(e.getKey()).containsKey(e.getValue()));
  }

  @ExceptionHandler(Exception.class)
  public ResponseEntity<Map<String, String>> failure(Exception e) {
    return ResponseEntity.status(HttpStatus.CONFLICT)
        .body(
            Map.of(
                "error",
                e.toString(),
                "recovery",
                "Inspect /api/status. Apps remain paused on transition failure; retry with the same"
                    + " force parameter."));
  }
}

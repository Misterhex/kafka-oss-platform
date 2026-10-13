package lab;

import com.fasterxml.jackson.databind.JsonNode;
import java.net.URI;
import java.net.http.*;
import java.nio.file.*;
import java.time.*;
import java.util.*;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigResource;
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
  private final Path statePath = Path.of(System.getenv().getOrDefault("STATE_DIR", "/state"), "state.json");
  private final HttpClient http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();
  private State state;
  private boolean force, externallyFenced;

  @com.fasterxml.jackson.annotation.JsonIgnoreProperties(ignoreUnknown = true)
  public static class State {
    public String phase = "NEW", primary = "k0x", error = "", target = "", reverseStatus = "NOT_STARTED";
    public Checkpoint checkpoint = new Checkpoint();
    public String sourceCluster = "", sourceCapturedAt = "";
    public Map<String, Map<String, Long>> sourceGroupOffsets = new TreeMap<>();
    public Map<String, Long> sourceEnds = new TreeMap<>(), targetEnds = new TreeMap<>();
    public Map<String, String> schemas = new TreeMap<>();
    public List<String> skipped = new ArrayList<>();
    public boolean forced = false, checkpointFrozen = false, sourceReachable = true;
  }

  public DrApplication() throws Exception {
    state = Files.exists(statePath) ? DurableJson.JSON.readValue(statePath.toFile(), State.class) : new State();
  }
  public static void main(String[] args) { SpringApplication.run(DrApplication.class, args); }
  private void save() throws Exception { DurableJson.write(statePath, state); }
  private void phase(String phase) throws Exception { state.phase = phase; save(); }
  static String other(String site) { return site.equals("k0x") ? "k0y" : "k0x"; }
  static String alias(String site) { return site.equals("k0x") ? "hk" : "sg"; }
  static String bootstrap(String site) { return site + ":19092"; }
  static String worker(String site) { return "http://connect-" + site + ":8083/connectors"; }
  static String name(String source, String target) { return alias(source) + "-to-" + alias(target); }
  private RegistryClient registry(String site) { return new RegistryClient("http://schema-registry-" + site + ":8081"); }

  private static class HttpFailure extends IllegalStateException {
    final int status;
    HttpFailure(int status, String message) { super(message); this.status = status; }
  }
  private String request(String method, String url, Object body) throws Exception {
    var b = HttpRequest.newBuilder(URI.create(url)).timeout(Duration.ofSeconds(15));
    b.header("Content-Type", "application/json").method(method, body == null
        ? HttpRequest.BodyPublishers.noBody() : HttpRequest.BodyPublishers.ofString(DurableJson.JSON.writeValueAsString(body)));
    var r = http.send(b.build(), HttpResponse.BodyHandlers.ofString());
    if (r.statusCode() / 100 != 2) throw new HttpFailure(r.statusCode(), method + " " + url + " => " + r.statusCode() + " " + r.body());
    return r.body();
  }
  private JsonNode json(String method, String url, Object body) throws Exception {
    return DurableJson.JSON.readTree(request(method, url, body));
  }
  private boolean exists(String url) throws Exception {
    try { request("GET", url + "/config", null); return true; }
    catch (HttpFailure e) { if (e.status == 404) return false; throw e; }
  }
  private void waitConnector(String url, String expected) throws Exception {
    long deadline = System.nanoTime() + Duration.ofSeconds(60).toNanos();
    while (System.nanoTime() < deadline) {
      try {
        var s = json("GET", url + "/status", null);
        boolean ok = s.path("connector").path("state").asText().equals(expected);
        if (expected.equals("RUNNING")) ok &= !s.path("tasks").isEmpty();
        for (var task : s.path("tasks")) {
          ok &= task.path("state").asText().equals(expected);
        }
        if (ok) return;
      } catch (HttpFailure e) { if (!Set.of(404, 409, 503).contains(e.status)) throw e; }
      Thread.sleep(250);
    }
    throw new IllegalStateException("Connector did not become " + expected + ": " + url);
  }

  private List<TopicPartition> partitions(String site) throws Exception {
    try (var admin = KafkaSupport.admin(bootstrap(site))) {
      var topics = admin.describeTopics(List.of(KafkaSupport.TOPIC, "_schemas")).allTopicNames().get(15, TimeUnit.SECONDS);
      return topics.values().stream().flatMap(t -> t.partitions().stream().map(p -> new TopicPartition(t.name(), p.partition()))).toList();
    }
  }
  static String partitionKey(TopicPartition p) { return p.topic() + ":" + p.partition(); }
  private Map<String, Long> ends(String site) throws Exception {
    var ps = partitions(site);
    try (var reader = KafkaSupport.reader(bootstrap(site))) {
      Map<String, Long> result = new TreeMap<>();
      reader.endOffsets(ps).forEach((p, o) -> result.put(partitionKey(p), o));
      return result;
    }
  }
  private Map<String, Long> stableEnds(String site) throws Exception {
    var before = ends(site);
    long deadline = System.nanoTime() + Duration.ofSeconds(20).toNanos();
    while (System.nanoTime() < deadline) {
      Thread.sleep(2000);
      var after = ends(site);
      if (before.equals(after)) return after;
      before = after;
    }
    throw new IllegalStateException("Source offsets still advancing after fencing");
  }

  /** Final retained schema/config/context values; modes remain local and NOOP has no logical state. */
  private Map<String, String> schemaState(String site) throws Exception {
    var properties = KafkaSupport.properties(bootstrap(site));
    properties.put("key.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
    properties.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
    properties.put("enable.auto.commit", "false");
    try (var c = new KafkaConsumer<byte[], byte[]>(properties)) {
      var p = new TopicPartition("_schemas", 0);
      c.assign(List.of(p)); c.seekToBeginning(List.of(p));
      long end = c.endOffsets(List.of(p)).get(p), deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
      Map<String, String> result = new TreeMap<>();
      while (c.position(p) < end) {
        if (System.nanoTime() > deadline) throw new IllegalStateException("Schema scan timed out");
        for (var r : c.poll(Duration.ofMillis(200))) {
          if (r.offset() >= end) continue;
          var key = DurableJson.JSON.readTree(r.key());
          if (Set.of("MODE", "NOOP").contains(key.path("keytype").asText())) continue;
          String encoded = Base64.getEncoder().encodeToString(r.key());
          if (r.value() == null) result.remove(encoded);
          else result.put(encoded, Base64.getEncoder().encodeToString(r.value()));
        }
      }
      return result;
    }
  }

  private Checkpoint capture(String site) throws Exception {
    var cp = new Checkpoint(); cp.capturedAt = Instant.now().toString();
    try (var c = KafkaSupport.reader(bootstrap(site))) {
      var ps = partitions(site).stream().filter(p -> p.topic().equals(KafkaSupport.TOPIC)).toList();
      var ends = c.endOffsets(ps); var beginnings = c.beginningOffsets(ps);
      for (var p : ps) {
        if (ends.get(p) > beginnings.get(p)) cp.barriers.put(p.partition(), readKey(c, p, ends.get(p) - 1));
      }
    }
    return cp;
  }
  /** Raw committed offsets for every source group; never translated or applied to another cluster. */
  static Map<String, Map<String, Long>> captureGroupOffsets(Admin admin) throws Exception {
    Map<String, Map<String, Long>> groups = new TreeMap<>();
    for (var group : admin.listConsumerGroups().all().get(15, TimeUnit.SECONDS)) {
      Map<String, Long> offsets = new TreeMap<>();
      admin.listConsumerGroupOffsets(group.groupId()).partitionsToOffsetAndMetadata()
          .get(15, TimeUnit.SECONDS).forEach((p, o) -> {
            if (o != null) offsets.put(partitionKey(p), o.offset());
          });
      groups.put(group.groupId(), offsets);
    }
    return groups;
  }

  private void recordSourceState(String source, Map<String, Long> topicEnds) throws Exception {
    try (var admin = KafkaSupport.admin(bootstrap(source))) {
      var groups = captureGroupOffsets(admin);
      state.sourceCluster = source;
      state.sourceCapturedAt = Instant.now().toString();
      state.sourceEnds = new TreeMap<>(topicEnds);
      state.sourceGroupOffsets = groups;
    }
    save();
  }

  private String readKey(KafkaConsumer<String, byte[]> c, TopicPartition p, long offset) {
    c.assign(List.of(p)); c.seek(p, offset);
    long deadline = System.nanoTime() + Duration.ofSeconds(10).toNanos();
    while (System.nanoTime() < deadline)
      for (var r : c.poll(Duration.ofMillis(200))) if (r.offset() == offset) return r.key();
    throw new IllegalStateException("No retained event at " + p + "/" + offset);
  }

  private Map<String, String> config(String source, String target, boolean checkpoints) {
    Map<String, String> c = new LinkedHashMap<>();
    c.put("connector.class", "org.apache.kafka.connect.mirror." + (checkpoints ? "MirrorCheckpointConnector" : "MirrorSourceConnector"));
    c.put("tasks.max", "1");
    c.put("source.cluster.alias", alias(source)); c.put("target.cluster.alias", alias(target));
    c.put("source.cluster.bootstrap.servers", bootstrap(source)); c.put("target.cluster.bootstrap.servers", bootstrap(target));
    for (String side : List.of("source", "target")) {
      c.put(side + ".cluster.security.protocol", "SASL_PLAINTEXT");
      c.put(side + ".cluster.sasl.mechanism", "PLAIN");
      c.put(side + ".cluster.sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"mirror\" password=\"mirror-lab\";");
    }
    c.put("replication.policy.class", "org.apache.kafka.connect.mirror.IdentityReplicationPolicy");
    c.put("offset-syncs.topic.location", "target"); c.put("replication.factor", "1");
    c.put("key.converter", "org.apache.kafka.connect.converters.ByteArrayConverter");
    c.put("value.converter", "org.apache.kafka.connect.converters.ByteArrayConverter");
    if (checkpoints) {
      c.put("groups", KafkaSupport.GROUP); c.put("topics", KafkaSupport.TOPIC);
      c.put("checkpoints.topic.replication.factor", "1"); c.put("emit.checkpoints.interval.seconds", "1");
      c.put("refresh.groups.interval.seconds", "1"); c.put("sync.group.offsets.enabled", "false");
    } else {
      c.put("topics", KafkaSupport.TOPIC + ",_schemas");
      c.put("topics.exclude", "__.*,.*[.-]internal,_connect-.*");
      c.put("offset-syncs.topic.replication.factor", "1"); c.put("refresh.topics.interval.seconds", "1");
      c.put("source.consumer.auto.offset.reset", "earliest"); c.put("sync.topic.acls.enabled", "false");
      c.put("sync.topic.configs.enabled", "false"); c.put("emit.heartbeats.enabled", "false");
      c.put("transforms", "localSchemaModes"); c.put("transforms.localSchemaModes.type", "lab.connect.KeepLocalSchemaModes");
    }
    return c;
  }
  private void compactSchemaTopic(String site) throws Exception {
    try (var admin = KafkaSupport.admin(bootstrap(site))) {
      try { admin.createTopics(List.of(new NewTopic("_schemas", 1, (short) 1).configs(Map.of("cleanup.policy", "compact")))).all().get(15, TimeUnit.SECONDS); }
      catch (java.util.concurrent.ExecutionException e) { if (!(e.getCause() instanceof org.apache.kafka.common.errors.TopicExistsException)) throw e; }
      var resource = new ConfigResource(ConfigResource.Type.TOPIC, "_schemas");
      admin.incrementalAlterConfigs(Map.of(resource, List.of(new AlterConfigOp(new ConfigEntry("cleanup.policy", "compact"), AlterConfigOp.OpType.SET)))).all().get(15, TimeUnit.SECONDS);
    }
  }
  private void stopForward(String source, String target) throws Exception {
    for (String suffix : List.of("-checkpoints", "")) {
      String url = worker(target) + "/" + name(source, target) + suffix;
      if (!exists(url)) continue;
      request("PUT", url + "/stop", null); waitConnector(url, "STOPPED");
      request("DELETE", url, null);
      if (exists(url)) throw new IllegalStateException("Forward connector still exists: " + url);
    }
  }

  /** Stage 1: centrally fence clients. No calls to application pause/resume endpoints. */
  void drain(String source, String target) throws Exception {
    phase("DRAIN");
    PlatformAccess.fence(bootstrap(target));
    // Do not interpret an unreachable source as a successful ACL update.
    try { PlatformAccess.fence(bootstrap(source)); }
    catch (Exception unavailable) {
      if (!force || !externallyFenced) throw new IllegalStateException("Cannot fence source. An unavailable site requires force=true and sourceFenced=true after external fencing", unavailable);
      state.sourceReachable = false;
      state.skipped.add("Source ACL update: external fence asserted");
      state.skipped.add("Source drain and fresh checkpoint: source unavailable; possible data loss");
      save(); return;
    }
    state.sourceReachable = true;
    registry(source).request("PUT", "/mode", Map.of("mode", "READONLY"));
    // ACL revocation is not a graceful application flush. Allow accepted requests and final commits to settle.
    recordSourceState(source, stableEnds(source));
    state.checkpoint = capture(source); state.checkpointFrozen = true;
    state.schemas = schemaState(source); save();
  }

  /** Stage 2: prove the drain, stop both connectors, and save destination boundaries before granting writes. */
  void sync(String source, String target) throws Exception {
    phase("SYNC");
    registry(target).request("PUT", "/mode", Map.of("mode", "READONLY"));
    if (state.sourceReachable) {
      long deadline = System.nanoTime() + Duration.ofSeconds(90).toNanos();
      while (!hasBarriers(KafkaSupport.scan(bootstrap(target)), state.checkpoint) || !state.schemas.equals(schemaState(target))) {
        if (System.nanoTime() > deadline) throw new IllegalStateException("Forward drain timed out; both application sites remain fenced");
        Thread.sleep(500);
      }
    }
    stopForward(source, target);
    state.targetEnds = ends(target); save();
    if (!state.schemas.equals(schemaState(target))) throw new IllegalStateException("Schema IDs/definitions differ from the last source snapshot; restore the original schema backup before promotion");
    // Verify the running registry has loaded the retained mappings, not merely that Kafka has them.
    for (var value : state.schemas.values()) {
      var record = DurableJson.JSON.readTree(Base64.getDecoder().decode(value));
      if (!record.has("schema") || record.path("deleted").asBoolean()) continue;
      int id = record.path("id").asInt();
      var actual = registry(target).request("GET", "/schemas/ids/" + id, null);
      if (!actual.path("schema").asText().equals(record.path("schema").asText()))
        throw new IllegalStateException("Target registry schema cache mismatch for ID " + id);
    }
    save();
  }

  /** Stage 3: enable target access, then establish reverse replication without blocking on an unavailable old site. */
  void flip(String source, String target) throws Exception {
    phase("FLIP");
    registry(target).request("PUT", "/mode", Map.of("mode", "READWRITE"));
    PlatformAccess.enable(bootstrap(target));
    state.primary = target; state.reverseStatus = "PENDING";
    phase(target.equals("k0x") ? "HK_ACTIVE" : "SG_ACTIVE");
    try { startReverse(source, target); state.reverseStatus = "RUNNING"; state.error = ""; }
    catch (Exception e) { state.error = "Promotion complete; reverse replication pending: " + e; }
    save();
  }

  static Map<String, Object> reverseOffsets(String sourceAlias, Map<String, Long> boundaries) {
    List<Map<String, Object>> offsets = new ArrayList<>();
    boundaries.forEach((key, next) -> {
      if (next < 0) throw new IllegalArgumentException("Negative offset boundary");
      int colon = key.lastIndexOf(':');
      Map<String, Object> entry = new LinkedHashMap<>();
      entry.put("partition", Map.of("cluster", sourceAlias, "topic", key.substring(0, colon), "partition", Integer.parseInt(key.substring(colon + 1))));
      entry.put("offset", next == 0 ? null : Map.of("offset", next - 1));
      offsets.add(entry);
    });
    return Map.of("offsets", offsets);
  }
  private void startReverse(String oldSource, String newSource) throws Exception {
    PlatformAccess.fence(bootstrap(oldSource));
    registry(oldSource).request("PUT", "/mode", Map.of("mode", "READONLY"));
    compactSchemaTopic(oldSource);
    if (!state.targetEnds.containsKey(KafkaSupport.TOPIC + ":0") || !state.targetEnds.containsKey("_schemas:0"))
      throw new IllegalStateException("Saved reverse boundaries are missing");
    try (var c = KafkaSupport.reader(bootstrap(newSource))) {
      var ps = partitions(newSource);
      var beginnings = c.beginningOffsets(ps); var currentEnds = c.endOffsets(ps);
      for (var p : ps) {
        Long boundary = state.targetEnds.get(partitionKey(p));
        if (boundary == null) throw new IllegalStateException("Missing reverse boundary for " + p);
        validateBoundary(boundary, beginnings.get(p), currentEnds.get(p));
      }
    }
    String url = worker(oldSource) + "/" + name(newSource, oldSource);
    // No retry/resume engine: refuse to reset an already running reverse connector.
    if (exists(url)) throw new IllegalStateException("Reverse connector already exists; inspect its offsets before recreating it");
    request("POST", worker(oldSource), Map.of("name", name(newSource, oldSource), "initial_state", "STOPPED", "config", config(newSource, oldSource, false)));
    waitConnector(url, "STOPPED");
    request("DELETE", url + "/offsets", null);
    request("PATCH", url + "/offsets", reverseOffsets(alias(newSource), state.targetEnds));
    var installed = json("GET", url + "/offsets", null);
    if (!offsetsMatch(installed, reverseOffsets(alias(newSource), state.targetEnds))) throw new IllegalStateException("Reverse source offsets not installed");
    request("PUT", url + "/resume", null); waitConnector(url, "RUNNING");
    String checkpoint = url + "-checkpoints";
    request("PUT", checkpoint + "/config", config(newSource, oldSource, true)); waitConnector(checkpoint, "RUNNING");
  }
  static void validateBoundary(long saved, long beginning, long end) {
    if (saved < beginning || saved > end)
      throw new IllegalStateException("Reverse boundary is outside retained source offsets; reseed the standby before replication");
  }
  static boolean offsetsMatch(JsonNode actual, Map<String, Object> expected) throws Exception {
    var wanted = DurableJson.JSON.readTree(DurableJson.JSON.writeValueAsBytes(expected)).path("offsets");
    Set<JsonNode> a = new HashSet<>(), b = new HashSet<>();
    actual.path("offsets").forEach(a::add); wanted.forEach(b::add);
    return a.equals(b);
  }

  @PostMapping("/api/promote")
  public synchronized State promote(@RequestParam String target,
      @RequestParam(defaultValue = "false") boolean force,
      @RequestParam(defaultValue = "false") boolean sourceFenced) throws Exception {
    Promotion.validate(state.primary, target);
    this.force = force; this.externallyFenced = sourceFenced;
    state.target = target; state.forced = force; state.skipped.clear(); state.error = "";
    state.sourceCluster = state.primary; state.sourceCapturedAt = ""; state.sourceGroupOffsets.clear();
    state.sourceEnds.clear(); state.targetEnds.clear(); state.reverseStatus = "NOT_STARTED";
    try {
      Promotion.promote(state.primary, target, new Promotion.Stages() {
        public void drain(String s, String t) throws Exception { DrApplication.this.drain(s, t); }
        public void sync(String s, String t) throws Exception { DrApplication.this.sync(s, t); }
        public void flip(String s, String t) throws Exception { DrApplication.this.flip(s, t); }
      });
      return state;
    } catch (Exception e) { state.error = e.toString(); save(); throw e; }
  }

  @PostMapping("/api/reverse")
  public synchronized State reverse() throws Exception {
    startReverse(other(state.primary), state.primary);
    state.reverseStatus = "RUNNING"; state.error = ""; save(); return state;
  }

  @PostMapping("/api/bootstrap")
  public synchronized State bootstrapLab() throws Exception {
    PlatformAccess.fence(bootstrap("k0y"));
    for (String site : List.of("k0x", "k0y")) { KafkaSupport.ensureTopic(bootstrap(site)); compactSchemaTopic(site); }
    // Refuse an opposing direction; never silently create an identity-policy replication loop.
    if (exists(worker("k0x") + "/sg-to-hk")) throw new IllegalStateException("Opposing connector exists");
    registry("k0x").request("PUT", "/mode", Map.of("mode", "READWRITE"));
    registry("k0y").request("PUT", "/mode", Map.of("mode", "READONLY"));
    for (boolean checkpoints : List.of(false, true)) {
      String url = worker("k0y") + "/hk-to-sg" + (checkpoints ? "-checkpoints" : "");
      request("PUT", url + "/config", config("k0x", "k0y", checkpoints)); waitConnector(url, "RUNNING");
    }
    PlatformAccess.enable(bootstrap("k0x"));
    state.primary = "k0x"; state.error = ""; phase("HK_ACTIVE"); return state;
  }
  @Scheduled(fixedDelay = 3000)
  public synchronized void checkpoint() {
    if (!Set.of("HK_ACTIVE", "SG_ACTIVE").contains(state.phase)) return;
    try {
      state.checkpoint = capture(state.primary); state.schemas = schemaState(state.primary);
      if (state.error.startsWith("Checkpoint:")) state.error = "";
      save();
    }
    catch (Exception e) { state.error = "Checkpoint: " + e; }
  }
  @PostMapping("/api/checkpoint") public synchronized State manualCheckpoint() throws Exception {
    recordSourceState(state.primary, ends(state.primary));
    state.checkpoint = capture(state.primary); state.schemas = schemaState(state.primary);
    if (state.error.startsWith("Checkpoint:")) state.error = "";
    save(); return state;
  }
  @GetMapping("/api/status") public synchronized State status() { return state; }
  @GetMapping("/api/apps") public Map<String, Object> appStatus() throws Exception {
    return Map.of(
        "producer", DurableJson.JSON.readValue(request("GET", "http://synthetic-producer:8080/status", null), Object.class),
        "consumer", DurableJson.JSON.readValue(request("GET", "http://synthetic-consumer:8080/status", null), Object.class));
  }
  @GetMapping("/api/records") public Map<Integer, LinkedHashMap<String, Long>> records(@RequestParam String cluster) {
    if (!Set.of("k0x", "k0y").contains(cluster)) throw new IllegalArgumentException("Unknown cluster");
    return KafkaSupport.scan(bootstrap(cluster));
  }
  @GetMapping("/api/clusters") public Map<String, String> clusters() throws Exception {
    try (var x = KafkaSupport.admin(bootstrap("k0x")); var y = KafkaSupport.admin(bootstrap("k0y"))) {
      return Map.of("k0x", x.describeCluster().clusterId().get(10, TimeUnit.SECONDS), "k0y", y.describeCluster().clusterId().get(10, TimeUnit.SECONDS));
    }
  }
  static boolean hasBarriers(Map<Integer, ? extends Map<String, Long>> target, Checkpoint cp) {
    return cp.barriers.entrySet().stream().allMatch(e -> target.containsKey(e.getKey()) && target.get(e.getKey()).containsKey(e.getValue()));
  }
  @ExceptionHandler(Exception.class) public ResponseEntity<Map<String, String>> failure(Exception e) {
    return ResponseEntity.status(HttpStatus.CONFLICT).body(Map.of("error", e.toString(), "recovery", "Inspect /api/status. No automatic retry or client restart is performed."));
  }
}

package lab.schema;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.nio.file.Path;
import java.util.*;
import lab.*;

/** REST import is a logical restore, not a replay of the backing topic. */
public final class SchemaApiRestore {
  private final RegistryClient registry;
  public SchemaApiRestore(String url) { registry = new RegistryClient(url); }
  record Schema(String subject, int version, ObjectNode body, boolean deleted) {
    String identity() { return subject + "/" + version; }
  }
  record Setting(String subject, ObjectNode body) { }
  record Plan(List<Schema> schemas, List<Setting> configs, List<Setting> modes) { }
  private static final Set<String> SCHEMA_FIELDS = Set.of("subject", "version", "id", "schemaType", "schema", "references", "metadata", "ruleSet", "deleted", "guid", "ts");
  private static final Set<String> CONFIG_FIELDS = Set.of("subject", "alias", "normalize", "compatibilityLevel", "compatibilityGroup", "defaultMetadata", "overrideMetadata", "defaultRuleSet", "overrideRuleSet");

  static Plan plan(SchemaLogBackup.Snapshot snapshot) throws Exception {
    var schemas = new TreeMap<String, Schema>();
    var configs = new ArrayList<Setting>(); var modes = new ArrayList<Setting>();
    var deletedSubjects = new HashMap<String, Integer>();
    for (var entry : SchemaLogBackup.logicalState(snapshot).entrySet()) {
      JsonNode key = DurableJson.JSON.readTree(SchemaLogBackup.decode(entry.getKey()));
      JsonNode value = DurableJson.JSON.readTree(SchemaLogBackup.decode(entry.getValue()));
      String type = key.path("keytype").asText();
      if (type.equals("NOOP")) continue;
      if (!(value instanceof ObjectNode object)) throw new IllegalArgumentException("Non-object registry value: " + key);
      String subject = key.path("subject").asText("");
      if (subject.startsWith(":")) throw new IllegalArgumentException("IMPORT restore currently supports the default context only; use Kafka replay for " + subject);
      switch (type) {
        case "SCHEMA" -> {
          fields(object, SCHEMA_FIELDS);
          if (subject.isBlank() || !subject.equals(object.path("subject").asText()) || key.path("version").asInt() != object.path("version").asInt()
              || object.path("version").asInt() < 1 || object.path("id").asInt() < 1 || !object.path("schema").isTextual())
            throw new IllegalArgumentException("Invalid schema identity: " + key);
          var body = object.deepCopy(); body.remove(List.of("subject", "deleted", "guid", "ts"));
          var schema = new Schema(subject, object.path("version").asInt(), body, object.path("deleted").asBoolean());
          schemas.put(schema.identity(), schema);
        }
        case "CONFIG" -> {
          fields(object, CONFIG_FIELDS);
          var body = object.deepCopy(); body.remove("subject");
          if (body.has("compatibilityLevel")) body.set("compatibility", body.remove("compatibilityLevel"));
          configs.add(new Setting(subject, body));
        }
        case "MODE" -> {
          fields(object, Set.of("subject", "mode"));
          if (!Set.of("READWRITE", "READONLY", "IMPORT").contains(object.path("mode").asText())) throw new IllegalArgumentException("Unsupported mode: " + object);
          var body = object.deepCopy(); body.remove("subject"); modes.add(new Setting(subject, body));
        }
        case "DELETE_SUBJECT" -> {
          fields(object, Set.of("subject", "version"));
          if (subject.isBlank() || !subject.equals(object.path("subject").asText()) || object.path("version").asInt() < 1)
            throw new IllegalArgumentException("Invalid subject deletion marker: " + key);
          deletedSubjects.put(subject, object.path("version").asInt());
        }
        default -> throw new IllegalArgumentException("Unsupported stored key type " + type + "; use Kafka replay for full state");
      }
    }
    schemas.replaceAll((identity, schema) -> schema.version() <= deletedSubjects.getOrDefault(schema.subject(), 0)
        ? new Schema(schema.subject(), schema.version(), schema.body(), true) : schema);
    var idDefinitions = new HashMap<Integer, ObjectNode>();
    for (var schema : schemas.values()) {
      var definition = schema.body().deepCopy(); definition.remove(List.of("id", "version"));
      var previous = idDefinitions.putIfAbsent(schema.body().path("id").asInt(), definition);
      if (previous != null && !previous.equals(definition)) throw new IllegalArgumentException("Backup contains conflicting definitions for schema ID " + schema.body().path("id").asInt());
    }
    var ordered = new ArrayList<Schema>(); var visiting = new HashSet<String>(); var visited = new HashSet<String>();
    for (var schema : schemas.values()) visit(schema, schemas, visiting, visited, ordered);
    return new Plan(ordered, configs, modes);
  }

  private static void fields(ObjectNode object, Set<String> allowed) {
    object.fieldNames().forEachRemaining(name -> { if (!allowed.contains(name)) throw new IllegalArgumentException("Unsupported stored field " + name + "; use Kafka replay"); });
  }
  private static void visit(Schema schema, Map<String, Schema> all, Set<String> visiting, Set<String> visited, List<Schema> ordered) {
    if (visited.contains(schema.identity())) return;
    if (!visiting.add(schema.identity())) throw new IllegalArgumentException("Cyclic schema references: " + schema.identity());
    for (var reference : schema.body().path("references")) {
      String identity = reference.path("subject").asText() + "/" + reference.path("version").asInt();
      Schema dependency = all.get(identity);
      if (dependency == null) throw new IllegalArgumentException("Missing referenced schema in backup: " + identity);
      visit(dependency, all, visiting, visited, ordered);
    }
    visiting.remove(schema.identity()); visited.add(schema.identity()); ordered.add(schema);
  }

  public void restore(Path file) throws Exception {
    restore(file, false);
  }

  public void restore(Path file, boolean allowChecksumMismatch) throws Exception {
    // Validate checksum and the complete import plan before any HTTP mutation.
    Plan plan = plan(SchemaLogBackup.load(file, allowChecksumMismatch));
    var contexts = registry.request("GET", "/contexts", null);
    for (var context : contexts) if (!context.asText().equals(".")) throw new IllegalStateException("Destination has custom contexts; use a fresh registry");
    var subjects = registry.request("GET", "/subjects?deleted=true", null);
    if (!subjects.isArray() || !subjects.isEmpty()) throw new IllegalStateException("IMPORT destination must have no subjects, including soft-deleted subjects; no data was changed");
    // Catch IDs still retained after subject removal before changing mode. The API remains
    // authoritative for races or other collision conditions and must never be bypassed.
    var existing = registry.request("GET", "/schemas?deleted=true", null);
    if (!existing.isArray() || !existing.isEmpty()) throw new IllegalStateException("IMPORT destination still contains schemas/IDs; use a fresh registry");
    var originalMode = registry.request("GET", "/mode", null);
    registry.request("PUT", "/mode", Map.of("mode", "IMPORT"));
    try {
      for (var schema : plan.schemas()) {
        var result = registry.request("POST", subjectPath(schema) + "/versions", schema.body());
        if (result.path("id").asInt() != schema.body().path("id").asInt()) throw new IllegalStateException("Imported ID differs for " + schema.identity());
      }
      // Configs go after registration so normalization/default rules cannot alter schemas.
      for (var setting : plan.configs()) registry.request("PUT", settingPath("config", setting.subject()), setting.body());
      registry.request("PUT", "/mode", Map.of("mode", "READWRITE"));
      var reversed = new ArrayList<>(plan.schemas()); Collections.reverse(reversed);
      for (var schema : reversed) if (schema.deleted()) registry.request("DELETE", subjectPath(schema) + "/versions/" + schema.version(), null);
      for (var schema : plan.schemas()) verify(schema);
      var expectedSubjects = new TreeSet<String>(); plan.schemas().forEach(s -> expectedSubjects.add(s.subject()));
      var actualSubjects = new TreeSet<String>(); registry.request("GET", "/subjects?deleted=true", null).forEach(s -> actualSubjects.add(s.asText()));
      if (!actualSubjects.equals(expectedSubjects)) throw new IllegalStateException("Destination subject set differs; concurrent registry writes may have occurred");
      for (var setting : plan.configs()) {
        var actual = registry.request("GET", settingPath("config", setting.subject()), null);
        var expected = setting.body().deepCopy(); if (expected.has("compatibility")) expected.set("compatibilityLevel", expected.remove("compatibility"));
        for (var field : iterable(expected.fieldNames())) if (!Objects.equals(expected.get(field), actual.get(field))) throw new IllegalStateException("Restored config differs: " + setting.subject() + "/" + field);
      }
      for (var setting : plan.modes()) if (!setting.subject().isEmpty()) {
        registry.request("PUT", settingPath("mode", setting.subject()) + (setting.body().path("mode").asText().equals("IMPORT") ? "?force=true" : ""), setting.body());
        if (!registry.request("GET", settingPath("mode", setting.subject()), null).equals(setting.body())) throw new IllegalStateException("Restored mode differs: " + setting.subject());
      }
      var global = plan.modes().stream().filter(s -> s.subject().isEmpty()).findFirst().<JsonNode>map(Setting::body).orElse(originalMode);
      registry.request("PUT", global.path("mode").asText().equals("IMPORT") ? "/mode?force=true" : "/mode", global);
      if (!registry.request("GET", "/mode", null).equals(global)) throw new IllegalStateException("Restored global mode differs");
      System.out.println("IMPORT RESTORE OK: " + plan.schemas().size() + " schema versions; original IDs, schemas, references, metadata/rules, deletions and supported settings verified. Destination registry is running.");
    } catch (Exception failure) {
      // An incomplete migration must not silently return to ordinary schema registration.
      try { registry.request("PUT", "/mode?force=true", Map.of("mode", "IMPORT")); }
      catch (Exception modeFailure) { failure.addSuppressed(modeFailure); }
      throw new IllegalStateException("IMPORT restore incomplete. Keep destination quiesced; use a fresh registry for retry. Mode reset to IMPORT was attempted.", failure);
    }
  }
  private void verify(Schema schema) throws Exception {
    var actual = registry.request("GET", subjectPath(schema) + "/versions/" + schema.version() + "?deleted=true", null);
    for (var field : iterable(schema.body().fieldNames())) if (!Objects.equals(schema.body().get(field), actual.get(field)))
      throw new IllegalStateException("Restored schema differs: " + schema.identity() + "/" + field);
    if (actual.path("deleted").asBoolean() != schema.deleted()) throw new IllegalStateException("Restored deletion state differs: " + schema.identity());
  }
  private static Iterable<String> iterable(Iterator<String> iterator) { return () -> iterator; }
  private static String subjectPath(Schema schema) { return "/subjects/" + RegistryClient.encode(schema.subject()); }
  private static String settingPath(String kind, String subject) { return "/" + kind + (subject.isEmpty() ? "" : "/" + RegistryClient.encode(subject)); }
}

package lab.schema;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.assumeTrue;
import java.nio.file.*;
import java.util.*;
import lab.*;
import org.junit.jupiter.api.Test;

/** Opt-in test against a dedicated, empty registry. Never targets HK/SG by default. */
class LiveSchemaApiRestoreTest {
  @Test void importsFixtureThroughRealRegistryApi() throws Exception {
    String url = System.getProperty("schema.import.url", "");
    assumeTrue(!url.isEmpty(), "Supply a dedicated empty test registry via -Dschema.import.url");
    var original = SchemaLogBackup.load(Path.of("../artifacts/schema-test/backup.json"));
    var entries = new ArrayList<SchemaLogBackup.Entry>();
    // This fixture includes a custom context; exercise all DEFAULT-context fixtures here.
    // Context support remains covered by the raw-log integration test.
    for (var entry : original.records()) {
      var key = DurableJson.JSON.readTree(SchemaLogBackup.decode(entry.key()));
      if (!key.path("keytype").asText().equals("CONTEXT") && !key.path("subject").asText().startsWith(":")) entries.add(entry);
    }
    var snapshot = new SchemaLogBackup.Snapshot(1, original.createdAt(), original.sourceClusterId(), original.sourceTopic(), original.beginningOffset(), original.endOffset(), entries);
    var payload = DurableJson.JSON.valueToTree(snapshot); var envelope = DurableJson.JSON.createObjectNode();
    envelope.put("format", "confluent-schema-registry-kafka-log-v1"); envelope.put("sha256", SchemaLogBackup.digest(payload)); envelope.set("snapshot", payload);
    Path file = Path.of("../artifacts/schema-import-test/backup.json"); Files.createDirectories(file.toAbsolutePath().getParent()); DurableJson.write(file, envelope);
    new SchemaApiRestore(url).restore(file);
    var registry = new RegistryClient(url);
    assertEquals("READONLY", registry.request("GET", "/mode", null).path("mode").asText());
    String backingTopic = System.getProperty("schema.import.topic", "");
    if (!backingTopic.isEmpty()) {
      registry.request("PUT", "/mode", Map.of("mode", "READWRITE"));
      registry.request("PUT", "/mode/shared-one", Map.of("mode", "READWRITE"));
      var log = new SchemaLogBackup("localhost:29092", backingTopic, new Properties());
      Path guarded = Path.of("../artifacts/schema-import-test/readonly-backup.json").toAbsolutePath();
      new SchemaRegistryBackup(url).withReadonly(log.capture(), () -> {
        assertEquals("READONLY", registry.request("GET", "/mode", null).path("mode").asText());
        assertEquals("READONLY", registry.request("GET", "/mode/shared-one", null).path("mode").asText());
        log.backup(guarded, true);
      });
      assertEquals("READWRITE", registry.request("GET", "/mode", null).path("mode").asText());
      assertEquals("READWRITE", registry.request("GET", "/mode/shared-one", null).path("mode").asText());
      assertEquals("READONLY", registry.request("GET", "/mode/shared-two", null).path("mode").asText());
      for (var entry : SchemaLogBackup.logicalState(SchemaLogBackup.load(guarded)).entrySet()) {
        var key = DurableJson.JSON.readTree(SchemaLogBackup.decode(entry.getKey()));
        if (key.path("keytype").asText().equals("MODE"))
          assertEquals("READONLY", DurableJson.JSON.readTree(SchemaLogBackup.decode(entry.getValue())).path("mode").asText());
      }
    }
    assertEquals("FULL_TRANSITIVE", registry.request("GET", "/config", null).path("compatibilityLevel").asText());
    assertEquals("NONE", registry.request("GET", "/config/history", null).path("compatibilityLevel").asText());
    assertTrue(registry.request("GET", "/config/history", null).path("normalize").asBoolean());
    assertEquals("READONLY", registry.request("GET", "/mode/shared-two", null).path("mode").asText());
    assertEquals(registry.request("GET", "/subjects/shared-one/versions/1", null).path("id"), registry.request("GET", "/subjects/shared-two/versions/1", null).path("id"));
    assertThrows(IllegalStateException.class, () -> registry.request("GET", "/subjects/hard-deleted/versions?deleted=true", null));
    assertTrue(registry.request("GET", "/subjects/soft-deleted/versions/1?deleted=true", null).path("deleted").asBoolean());
    assertEquals("test-event", new EventCodec(url).decode(Files.readAllBytes(Path.of("../artifacts/schema-test/event.bin"))).path("id").asText());
    assertThrows(IllegalStateException.class, () -> new SchemaApiRestore(url).restore(file));
    assertEquals(backingTopic.isEmpty() ? "READONLY" : "READWRITE", registry.request("GET", "/mode", null).path("mode").asText());
  }
}

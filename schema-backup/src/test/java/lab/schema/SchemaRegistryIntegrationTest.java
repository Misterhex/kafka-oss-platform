package lab.schema;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.assumeTrue;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.nio.file.*;
import java.util.*;
import lab.*;
import org.junit.jupiter.api.Test;

/** Opt-in, two-phase live test; the shell only handles container lifecycle. */
class SchemaRegistryIntegrationTest {
  private final RegistryClient source = new RegistryClient("http://localhost:8089");
  private final RegistryClient target = new RegistryClient("http://localhost:8088");
  private final Path directory = Path.of("../artifacts/schema-test").toAbsolutePath().normalize();
  private static final String[] FIXTURES = {"shared-one", "shared-two", "reference-base", "reference-parent", "history", "soft-deleted", "hard-deleted", "json-example", "proto-example", ":.audit:audit-event"};

  @Test void seedAndRestoreOrVerifyAfterRegistryStarts() throws Exception {
    String phase = System.getProperty("schema.integration", "");
    assumeTrue(!phase.isEmpty(), "Run scripts/test-schema.sh for live Schema Registry tests");
    Files.createDirectories(directory);
    if (phase.equals("seed")) {
      assertEquals(0, source.request("GET", "/subjects?deleted=true", null).size(), "Fixture registry must be fresh");
      var codec = new EventCodec("http://localhost:8089"); codec.registerFive();
      assertEquals(5,codec.registered().size());
      String shared = "{\"type\":\"record\",\"name\":\"Shared\",\"fields\":[{\"name\":\"id\",\"type\":\"string\"}]}";
      int id1 = source.register("shared-one",shared), id2 = source.register("shared-two",shared);
      assertEquals(id1,id2,"Same schema in two subjects must retain shared ID");
      source.register("reference-base","{\"type\":\"record\",\"name\":\"Address\",\"namespace\":\"fixture\",\"fields\":[{\"name\":\"city\",\"type\":\"string\"}]}");
      source.request("POST","/subjects/reference-parent/versions",Map.of("schemaType","AVRO","schema","{\"type\":\"record\",\"name\":\"Envelope\",\"namespace\":\"fixture\",\"fields\":[{\"name\":\"address\",\"type\":\"fixture.Address\"}]}","references",List.of(Map.of("name","fixture.Address","subject","reference-base","version",1))));
      source.request("PUT","/config/history",Map.of("compatibility","NONE","normalize",true));
      source.register("history","\"string\""); source.register("history","\"int\""); source.register("history","\"long\"");
      source.request("DELETE","/subjects/history/versions/2",null);
      source.register("soft-deleted","\"boolean\"");source.request("DELETE","/subjects/soft-deleted",null);
      source.register("hard-deleted","\"double\"");source.request("DELETE","/subjects/hard-deleted",null);source.request("DELETE","/subjects/hard-deleted?permanent=true",null);
      source.request("POST","/subjects/json-example/versions",Map.of("schemaType","JSON","schema","{\"type\":\"object\",\"properties\":{\"id\":{\"type\":\"string\"}}}","metadata",Map.of("properties",Map.of("owner","schema-test")),"ruleSet",Map.of("domainRules",List.of(Map.of("name","permit","kind","CONDITION","type","CEL","mode","WRITE","expr","true","disabled",true)))));
      source.request("POST","/subjects/proto-example/versions",Map.of("schemaType","PROTOBUF","schema","syntax = \"proto3\"; package fixture; message Example { string id = 1; }"));
      source.register(":.audit:audit-event","\"bytes\"");
      source.request("PUT","/config/" + RegistryClient.encode(":.audit:"),Map.of("compatibility","FULL"));
      source.request("PUT","/mode/shared-two",Map.of("mode","READONLY"));
      source.request("PUT","/config",Map.of("compatibility","FULL_TRANSITIVE"));
      source.request("PUT","/mode",Map.of("mode","READONLY"));
      DurableJson.write(directory.resolve("expected.json"),state(source));
      Files.write(directory.resolve("event.bin"),codec.encode("test-event",5,"2026-10-11T00:00:00Z"));
      var service = new SchemaLogBackup("localhost:19092","_schemas_fixture",new Properties());
      service.backup(directory.resolve("backup.json"),true);
      var restore = new SchemaLogBackup("localhost:29092","_schemas_restore",new Properties());
      restore.restore(directory.resolve("backup.json"),(short)1);
      assertThrows(IllegalStateException.class,()->restore.restore(directory.resolve("backup.json"),(short)1),"Must reject a nonempty target");
    } else if (phase.equals("verify")) {
      var expected = DurableJson.JSON.readTree(directory.resolve("expected.json").toFile());
      assertEquals(expected,state(target),"All accessible registry state must survive restore");
      var event = new EventCodec("http://localhost:8088").decode(Files.readAllBytes(directory.resolve("event.bin")));
      assertEquals("test-event",event.path("id").asText());assertEquals(5,event.path("seqNo").asLong());
      // A new ID must not collide after rebuilding the registry's allocation state.
      target.request("PUT","/mode",Map.of("mode","READWRITE"));
      int newId = target.register("after-restore","{\"type\":\"fixed\",\"name\":\"NewId\",\"size\":8}");
      int maximum = 0;
      for (JsonNode subject : expected.path("subjects"))
        for (JsonNode version : subject.path("versions")) maximum = Math.max(maximum,version.path("id").asInt());
      assertTrue(newId>maximum,"Restored schema ID allocator must advance beyond existing IDs");
      target.request("DELETE","/subjects/after-restore",null);
      target.request("DELETE","/subjects/after-restore?permanent=true",null);
      target.request("PUT","/mode",Map.of("mode","READONLY"));
    } else fail("Unknown integration phase: " + phase);
  }

  private ObjectNode state(RegistryClient registry) throws Exception {
    var result = DurableJson.JSON.createObjectNode();
    result.set("config",registry.request("GET","/config",null));
    result.set("mode",registry.request("GET","/mode",null));
    result.set("contexts",registry.request("GET","/contexts",null));
    result.set("auditConfig",registry.request("GET","/config/" + RegistryClient.encode(":.audit:"),null));
    result.set("historyConfig",registry.request("GET","/config/history",null));
    result.set("sharedMode",registry.request("GET","/mode/shared-two",null));
    var subjects = result.putArray("subjects");
    var names = new TreeSet<String>(List.of(FIXTURES));
    for (String type : EventCodec.TYPES) names.add("appTopic-lab.events."+type);
    for (String subject : names) {
      if (subject.equals("hard-deleted")) {
        assertThrows(IllegalStateException.class,()->registry.request("GET","/subjects/hard-deleted/versions?deleted=true",null));
        continue;
      }
      var info = subjects.addObject();info.put("subject",subject);
      var versions = info.putArray("versions");
      for (JsonNode version : registry.request("GET","/subjects/"+RegistryClient.encode(subject)+"/versions?deleted=true",null)) {
        var schema = (ObjectNode) registry.request("GET","/subjects/"+RegistryClient.encode(subject)+"/versions/"+version.asInt()+"?deleted=true",null);
        schema.remove("ts"); // Registration time is operational metadata, not schema identity.
        versions.add(schema);
      }
    }
    result.set("activeSubjects",registry.request("GET","/subjects",null));
    return result;
  }
}

package lab.schema;

import static org.junit.jupiter.api.Assertions.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;
import lab.DurableJson;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class SchemaLogBackupTest {
  @Test void backupRejectsRegistryUrlBeforeAccessingTheRegistry() {
    var error = assertThrows(IllegalArgumentException.class,
        () -> new SchemaBackupApplication().run(new org.springframework.boot.DefaultApplicationArguments(
            "backup", "--bootstrap=localhost:1", "--file=backup.json", "--registry-url=http://localhost:1")));
    assertEquals("Unknown option: registry-url", error.getMessage());
  }

  @TempDir Path directory;
  private SchemaLogBackup.Entry entry(long offset, String key, String value) {
    return new SchemaLogBackup.Entry(offset, 1234,
        SchemaLogBackup.encode(key.getBytes(StandardCharsets.UTF_8)),
        value == null ? null : SchemaLogBackup.encode(value.getBytes(StandardCharsets.UTF_8)), List.of());
  }
  private SchemaLogBackup.Snapshot snapshot(List<SchemaLogBackup.Entry> records) {
    return new SchemaLogBackup.Snapshot(1, "now", "cluster", "_schemas", 0, 10, records);
  }
  @Test void retainsLatestValueAndHonorsTombstones() {
    var state = SchemaLogBackup.logicalState(snapshot(List.of(entry(0,"schema","v1"), entry(2,"schema","v2"), entry(3,"deleted","old"), entry(8,"deleted",null))));
    assertEquals(Map.of(SchemaLogBackup.encode("schema".getBytes(StandardCharsets.UTF_8)), SchemaLogBackup.encode("v2".getBytes(StandardCharsets.UTF_8))), state);
  }
  @Test void compactionGapsAreValidButReorderingAndOutOfBoundsAreRejected() {
    assertDoesNotThrow(() -> SchemaLogBackup.validate(snapshot(List.of(entry(2,"a","a"),entry(8,"b","b")))));
    assertThrows(IllegalArgumentException.class, () -> SchemaLogBackup.validate(snapshot(List.of(entry(8,"a","a"),entry(2,"b","b")))));
    assertThrows(IllegalArgumentException.class, () -> SchemaLogBackup.validate(snapshot(List.of(entry(10,"a","a")))));
  }
  @Test void rejectsTamperedBackupBeforeRestore() throws Exception {
    var payload = DurableJson.JSON.valueToTree(snapshot(List.of(entry(0,"key","value"))));
    var envelope = DurableJson.JSON.createObjectNode();
    envelope.put("format","confluent-schema-registry-kafka-log-v1");
    envelope.put("sha256",SchemaLogBackup.digest(payload)); envelope.set("snapshot",payload);
    Path file = directory.resolve("backup.json"); DurableJson.write(file,envelope);
    assertEquals(1,SchemaLogBackup.load(file).records().size());
    envelope.withObject("snapshot").put("endOffset",99); DurableJson.write(file,envelope);
    assertThrows(IllegalArgumentException.class, () -> SchemaLogBackup.load(file));
    assertEquals(99,SchemaLogBackup.load(file,true).endOffset());
    envelope.withObject("snapshot").put("endOffset",0);DurableJson.write(file,envelope);
    assertThrows(IllegalArgumentException.class,()->SchemaLogBackup.load(file,true),"Checksum bypass must still reject invalid record boundaries");
  }
  @Test void bytesAndNullValuesRoundTripWithoutUtf8Conversion() {
    byte[] bytes = {(byte)255, 0, 10, (byte)128};
    assertArrayEquals(bytes,SchemaLogBackup.decode(SchemaLogBackup.encode(bytes)));
    assertNull(SchemaLogBackup.decode(SchemaLogBackup.encode(null)));
  }
  @Test void rejectsNullKeysBeforeRestoreWritesAnything() {
    var record = new SchemaLogBackup.Entry(0,0,null,"AA==",List.of());
    assertThrows(IllegalArgumentException.class,()->SchemaLogBackup.validate(snapshot(List.of(record))));
  }
}

package lab.schema;

import com.fasterxml.jackson.databind.JsonNode;
import java.util.*;
import lab.*;

/** Temporarily freeze registry modes while the Kafka log is captured. */
final class SchemaRegistryBackup {
  @FunctionalInterface interface BackupAction { void run() throws Exception; }
  private record Mode(String subject, String value) { }
  private final RegistryClient registry;
  SchemaRegistryBackup(String url) { registry = new RegistryClient(url); }

  void withReadonly(SchemaLogBackup.Snapshot before, BackupAction backup) throws Exception {
    var originals = new ArrayList<Mode>();
    originals.add(new Mode("", registry.request("GET", "/mode", null).path("mode").asText()));
    // Include stored overrides even for subjects without registered schemas. Merely
    // listing /subjects misses these overrides, which can bypass global READONLY.
    for (var entry : SchemaLogBackup.logicalState(before).entrySet()) {
      JsonNode key = DurableJson.JSON.readTree(SchemaLogBackup.decode(entry.getKey()));
      if (!key.path("keytype").asText().equals("MODE") || key.path("subject").asText().isEmpty()) continue;
      String subject = key.path("subject").asText();
      String stored = DurableJson.JSON.readTree(SchemaLogBackup.decode(entry.getValue())).path("mode").asText();
      String actual = registry.request("GET", path(subject), null).path("mode").asText();
      if (!actual.equals(stored)) throw new IllegalStateException("Registry mode changed or URL/topic mismatch for " + subject + "; retry with writers quiesced");
      originals.add(new Mode(subject, actual));
    }
    for (var mode : originals) if (!Set.of("READWRITE", "READONLY", "IMPORT").contains(mode.value()))
      throw new IllegalStateException("Unsupported source mode " + mode.value() + "; no modes changed");
    var changed = new ArrayList<Mode>();
    Exception failure = null;
    try {
      for (var mode : originals) {
        if (!mode.value().equals("READONLY")) {
          // Track before PUT so even an ambiguous transport failure triggers cleanup.
          changed.add(mode); set(mode.subject(), "READONLY");
        }
        if (!registry.request("GET", path(mode.subject()), null).path("mode").asText().equals("READONLY"))
          throw new IllegalStateException("Source mode did not become READONLY: " + mode.subject());
      }
      System.out.println("Source registry is READONLY (including stored mode overrides); capturing backup.");
      backup.run();
    } catch (Exception error) { failure = error; }
    finally {
      Collections.reverse(changed);
      for (var mode : changed) {
        try { set(mode.subject(), mode.value()); }
        catch (Exception error) {
          var cleanup = new IllegalStateException("Could not restore source mode " + mode.value() + " for " + (mode.subject().isEmpty() ? "global scope" : mode.subject()) + "; check registry modes manually", error);
          if (failure == null) failure = cleanup; else failure.addSuppressed(cleanup);
        }
      }
    }
    if (failure != null) throw failure;
    System.out.println("Source registry modes restored to their previous values.");
  }
  private void set(String subject, String mode) throws Exception {
    registry.request("PUT", path(subject) + (mode.equals("IMPORT") ? "?force=true" : ""), Map.of("mode", mode));
    if (!registry.request("GET", path(subject), null).path("mode").asText().equals(mode))
      throw new IllegalStateException("Mode verification failed for " + subject);
  }
  private static String path(String subject) { return "/mode" + (subject.isEmpty() ? "" : "/" + RegistryClient.encode(subject)); }
}

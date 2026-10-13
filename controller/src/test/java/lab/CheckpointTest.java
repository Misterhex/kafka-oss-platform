package lab;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.*;
import java.util.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class CheckpointTest {
  private Map<Integer, Map<String, Long>> target() {
    return Map.of(0, Map.of("a", 17L));
  }

  @Test
  void drainWaitsForTheBarrierEvent() {
    var cp = new Checkpoint();
    cp.barriers.put(0, "missing");
    assertFalse(DrApplication.hasBarriers(target(), cp));
    cp.barriers.clear();
    cp.barriers.put(0, "a");
    assertTrue(DrApplication.hasBarriers(target(), cp));
  }

  @Test
  void durableStateRoundTripsAndReplaces(@TempDir Path dir) throws Exception {
    var path = dir.resolve("state.json");
    var state = new DrApplication.State();
    state.checkpoint.barriers.put(0, "a");
    state.sourceCluster = "k0x";
    state.sourceCapturedAt = "2026-10-13T00:00:00Z";
    state.sourceEnds.put("appTopic:0", 100L);
    state.sourceGroupOffsets.put("dr-consumer", Map.of("appTopic:0", 90L));
    DurableJson.write(path, state);
    state.phase = "PROMOTING";
    state.checkpointFrozen = true;
    DurableJson.write(path, state);
    var saved = DurableJson.JSON.readValue(path.toFile(), DrApplication.State.class);
    assertEquals("PROMOTING", saved.phase);
    assertTrue(saved.checkpointFrozen);
    assertEquals("a", saved.checkpoint.barriers.get(0));
    assertEquals("k0x", saved.sourceCluster);
    assertEquals(100L, saved.sourceEnds.get("appTopic:0"));
    assertEquals(90L, saved.sourceGroupOffsets.get("dr-consumer").get("appTopic:0"));
    assertFalse(Files.exists(dir.resolve("state.json.tmp")));
  }
  @Test void loadsLegacyStateWithoutRetainingTranslatedOffsets() throws Exception {
    var saved = DurableJson.JSON.readValue("""
        {"phase":"HK_ACTIVE","primary":"k0x","restoredOffsets":{"0":18},
         "checkpoint":{"capturedAt":"old","sourceOffsets":{"0":900},
                       "committedIds":{"0":"a"},"barriers":{"0":"last"}}}
        """, DrApplication.State.class);
    assertEquals("last", saved.checkpoint.barriers.get(0));
    var json = DurableJson.JSON.valueToTree(saved);
    assertFalse(json.has("restoredOffsets"));
    assertFalse(json.path("checkpoint").has("committedIds"));
    assertFalse(json.path("checkpoint").has("sourceOffsets"));
  }
}

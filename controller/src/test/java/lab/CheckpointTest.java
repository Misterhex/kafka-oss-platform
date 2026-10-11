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
  void translatesByIdentityNotSourceOffset() {
    var cp = new Checkpoint();
    cp.committedIds.put(0, "a");
    cp.sourceOffsets.put(0, 900L);
    assertEquals(Map.of(0, 18L), Checkpoint.translate(cp, target(), true));
  }

  @Test
  void plannedRecoveryRejectsMissingCommittedEvent() {
    var cp = new Checkpoint();
    cp.committedIds.put(0, "missing");
    assertThrows(IllegalStateException.class, () -> Checkpoint.translate(cp, target(), true));
  }

  @Test
  void forcedRecoveryReplaysWhenCheckpointIsMissing() {
    var cp = new Checkpoint();
    cp.committedIds.put(0, "missing");
    assertEquals(0L, Checkpoint.translate(cp, target(), false).get(0));
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
    state.checkpoint.committedIds.put(0, "a");
    DurableJson.write(path, state);
    state.phase = "PROMOTING";
    state.checkpointFrozen = true;
    DurableJson.write(path, state);
    var saved = DurableJson.JSON.readValue(path.toFile(), DrApplication.State.class);
    assertEquals("PROMOTING", saved.phase);
    assertTrue(saved.checkpointFrozen);
    assertEquals("a", saved.checkpoint.committedIds.get(0));
    assertFalse(Files.exists(dir.resolve("state.json.tmp")));
  }
}

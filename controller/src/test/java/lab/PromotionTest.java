package lab;

import static org.junit.jupiter.api.Assertions.*;
import java.util.*;
import org.junit.jupiter.api.Test;

class PromotionTest {
  @Test void sameThreeStagesInEitherDirection() throws Exception {
    for (String source : List.of("k0x", "k0y")) {
      var calls = new ArrayList<String>();
      Promotion.promote(source, DrApplication.other(source), new Promotion.Stages() {
        public void drain(String s, String t) { calls.add("drain:" + s + ":" + t); }
        public void sync(String s, String t) { calls.add("sync:" + s + ":" + t); }
        public void flip(String s, String t) { calls.add("flip:" + s + ":" + t); }
      });
      assertEquals(List.of("drain:" + source + ":" + DrApplication.other(source), "sync:" + source + ":" + DrApplication.other(source), "flip:" + source + ":" + DrApplication.other(source)), calls);
    }
  }
  @Test void failedSyncNeverEnablesTarget() {
    var calls = new ArrayList<String>();
    assertThrows(IllegalStateException.class, () -> Promotion.promote("k0x", "k0y", new Promotion.Stages() {
      public void drain(String s, String t) { calls.add("drain"); }
      public void sync(String s, String t) { calls.add("sync"); throw new IllegalStateException("not drained"); }
      public void flip(String s, String t) { fail("Must remain fenced"); }
    }));
    assertEquals(List.of("drain", "sync"), calls);
  }
  @Test void rejectsInvalidOrCurrentTargetBeforeFencing() {
    assertThrows(IllegalArgumentException.class, () -> Promotion.promote("k0x", "bad", null));
    assertThrows(IllegalArgumentException.class, () -> Promotion.promote("k0x", "k0x", null));
  }
  @Test void reverseStartsAtSavedNextOffsetsIncludingEmptyPartitions() throws Exception {
    var result = DurableJson.JSON.valueToTree(DrApplication.reverseOffsets("sg", Map.of("appTopic:0", 120L, "_schemas:0", 0L)));
    var records = result.path("offsets");
    for (var r : records) {
      assertEquals("sg", r.path("partition").path("cluster").asText());
      if (r.path("partition").path("topic").asText().equals("appTopic")) assertEquals(119, r.path("offset").path("offset").asLong());
      else assertTrue(r.path("offset").isNull());
    }
    assertThrows(IllegalArgumentException.class, () -> DrApplication.reverseOffsets("sg", Map.of("appTopic:0", -1L)));
  }
  @Test void verifiesReverseOffsetsAcrossJsonNumericRepresentations() throws Exception {
    var expected = DrApplication.reverseOffsets("sg", Map.of("appTopic:0", 120L));
    var actual = DurableJson.JSON.readTree(DurableJson.JSON.writeValueAsBytes(expected));
    assertTrue(DrApplication.offsetsMatch(actual, expected));
    ((com.fasterxml.jackson.databind.node.ObjectNode) actual.path("offsets").get(0).path("offset")).put("offset", 120);
    assertFalse(DrApplication.offsetsMatch(actual, expected));
  }
  @Test void decodesActualMirrorCheckpointWireLayout() throws Exception {
    var keyBytes = new java.io.ByteArrayOutputStream();
    try (var key = new java.io.DataOutputStream(keyBytes)) {
      key.writeUTF("dr-consumer"); key.writeUTF("appTopic"); key.writeInt(0);
    }
    var valueBytes = new java.io.ByteArrayOutputStream();
    try (var value = new java.io.DataOutputStream(valueBytes)) {
      value.writeShort(0); value.writeLong(900); value.writeLong(120); value.writeUTF("");
    }
    var checkpoint = DrApplication.decodeCheckpoint(keyBytes.toByteArray(), valueBytes.toByteArray());
    assertEquals("dr-consumer", checkpoint.group()); assertEquals("appTopic", checkpoint.topic());
    assertEquals(0, checkpoint.partition()); assertEquals(120, checkpoint.downstream());
    byte[] unsupported = valueBytes.toByteArray(); unsupported[1] = 1;
    assertThrows(IllegalStateException.class, () -> DrApplication.decodeCheckpoint(keyBytes.toByteArray(), unsupported));
  }
  @Test void refusesExpiredOrRecreatedSourceBoundaries() {
    assertDoesNotThrow(() -> DrApplication.validateBoundary(120, 120, 130));
    assertDoesNotThrow(() -> DrApplication.validateBoundary(0, 0, 0));
    assertThrows(IllegalStateException.class, () -> DrApplication.validateBoundary(119, 120, 130));
    assertThrows(IllegalStateException.class, () -> DrApplication.validateBoundary(120, 0, 10));
  }
  @Test void fencingDoesNotTouchPlatformOrMirrorIdentity() {
    for (var binding : PlatformAccess.denies()) assertTrue(Set.of("User:producer", "User:consumer").contains(binding.entry().principal()));
    assertEquals(3, PlatformAccess.denies().size());
  }
}

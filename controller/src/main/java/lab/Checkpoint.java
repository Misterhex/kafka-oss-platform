package lab;

import java.util.*;

public class Checkpoint {
  public String capturedAt = "";
  public Map<Integer, String> committedIds = new TreeMap<>();
  public Map<Integer, Long> sourceOffsets = new TreeMap<>();
  public Map<Integer, String> barriers = new TreeMap<>();

  public static Map<Integer, Long> translate(
      Checkpoint cp, Map<Integer, ? extends Map<String, Long>> target, boolean strict) {
    Map<Integer, Long> offsets = new TreeMap<>();
    for (int i = 0; i < KafkaSupport.PARTITIONS; i++) {
      String id = cp.committedIds.get(i);
      Long matched = id == null ? null : target.get(i).get(id);
      if (strict && id != null && matched == null)
        throw new IllegalStateException("Checkpoint event missing in target partition " + i);
      // A missing checkpoint in forced recovery replays retained data, never blindly copies a
      // source offset.
      offsets.put(i, matched == null ? 0L : matched + 1);
    }
    return offsets;
  }
}

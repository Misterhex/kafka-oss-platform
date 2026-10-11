package lab.schema;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.assumeTrue;
import java.time.Duration;
import java.util.*;
import lab.*;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;

/** Read-only smoke test for the current SG-primary lab; no failover or offset changes. */
class LiveAvroReplicationTest {
  @Test void allFiveAvroTypesReplicateAndDecodeWithRestoredHkRegistry() throws Exception {
    assumeTrue(Boolean.getBoolean("schema.runtime"), "Requires running SG-primary Avro lab");
    var sgCodec = new EventCodec("http://localhost:8087");
    var hkCodec = new EventCodec("http://localhost:8086");
    var tp = new TopicPartition("appTopic",0);
    Map<String,byte[]> samples = new HashMap<>();
    Set<String> types = new HashSet<>();
    try (var sg = KafkaSupport.reader("localhost:29092")) {
      sg.assign(List.of(tp));
      long end = sg.endOffsets(List.of(tp)).get(tp);
      long beginning = sg.beginningOffsets(List.of(tp)).get(tp);
      sg.seek(tp,Math.max(beginning,end-20));
      long deadline = System.nanoTime()+Duration.ofSeconds(20).toNanos();
      while (sg.position(tp)<end) {
        assertTrue(System.nanoTime()<deadline,"Source sampling timed out");
        for (var record : sg.poll(Duration.ofMillis(200))) {
          if (record.offset()>=end) continue;
          var event = sgCodec.decode(record.value());
          assertEquals(record.key(),event.path("id").asText());
          assertTrue(event.path("seqNo").asLong()>0);
          types.add(event.path("eventType").asText());samples.put(record.key(),record.value());
        }
      }
    }
    assertEquals(new HashSet<>(EventCodec.TYPES),types);
    try (var hk = KafkaSupport.reader("localhost:19092")) {
      hk.assign(List.of(tp));
      long end = hk.endOffsets(List.of(tp)).get(tp);
      long beginning = hk.beginningOffsets(List.of(tp)).get(tp);
      hk.seek(tp,Math.max(beginning,end-2000));
      long deadline = System.nanoTime()+Duration.ofSeconds(45).toNanos();
      while (!samples.isEmpty()) {
        assertTrue(System.nanoTime()<deadline,"Missing mirrored event IDs: "+samples.keySet());
        for (var record : hk.poll(Duration.ofMillis(200))) {
          byte[] expected = samples.remove(record.key());
          if (expected == null) continue;
          assertArrayEquals(expected,record.value(),"MM2 must preserve schema ID and Avro bytes");
          assertEquals(record.key(),hkCodec.decode(record.value()).path("id").asText());
        }
      }
    }
  }
}

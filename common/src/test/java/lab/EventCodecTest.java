package lab;

import static org.junit.jupiter.api.Assertions.*;
import java.nio.ByteBuffer;
import java.util.*;
import org.apache.avro.generic.*;
import org.apache.avro.io.*;
import org.junit.jupiter.api.Test;

class EventCodecTest {
  @Test void fiveDifferentSchemasEncodeStableIdsAndSequenceNumbers() throws Exception {
    Set<String> names = new HashSet<>();
    for (int i=0; i<5; i++) {
      var schema = EventCodec.schema(i); names.add(schema.getFullName());
      var record = new GenericData.Record(schema);
      record.put("id","event");record.put("seqNo",42L);record.put("createdAt","now");record.put("eventType",EventCodec.TYPES.get(i));
      record.put(schema.getFields().getLast().name(),"detail");
      byte[] encoded = EventCodec.encodeRecord(77,record);
      assertEquals(0,encoded[0]);assertEquals(77,ByteBuffer.wrap(encoded,1,4).getInt());
      var restored = new GenericDatumReader<GenericRecord>(schema).read(null,DecoderFactory.get().binaryDecoder(encoded,5,encoded.length-5,null));
      assertEquals(42L,restored.get("seqNo"));assertEquals("event",restored.get("id").toString());
    }
    assertEquals(5,names.size());
  }
  @Test void legacyJsonStillDecodesWithoutRegistryAccess() throws Exception {
    var codec = new EventCodec("http://127.0.0.1:1");
    assertEquals(2,codec.decode("{\"id\":\"old\",\"seqNo\":2}".getBytes(java.nio.charset.StandardCharsets.UTF_8)).path("seqNo").asLong());
    assertThrows(IllegalArgumentException.class,()->codec.decode(new byte[]{0,1}));
  }
}

package lab.connect;

import static org.junit.jupiter.api.Assertions.*;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.Test;

class KeepLocalSchemaModesTest {
  private SourceRecord record(String topic, String key, byte[] value) {
    return new SourceRecord(Map.of("topic",topic),Map.of("offset",1),topic,0,null,key.getBytes(StandardCharsets.UTF_8),null,value);
  }
  private KeepLocalSchemaModes<SourceRecord> transform() {
    var transform = new KeepLocalSchemaModes<SourceRecord>(); transform.configure(Map.of()); return transform;
  }
  @Test void dropsGlobalAndScopedModesIncludingTheirTombstones() {
    try(var transform = transform()) {
      assertNull(transform.apply(record("_schemas","{\"keytype\":\"MODE\",\"magic\":0}",new byte[]{1})));
      assertNull(transform.apply(record("_schemas_extra","{\"keytype\":\"MODE\",\"subject\":\"a\"}",null)));
    }
  }
  @Test void preservesSchemaConfigDeletionNoopAndTombstonesUnchanged() {
    try(var transform = transform()) {
      for(String type:new String[]{"SCHEMA","CONFIG","DELETE_SUBJECT","CLEAR_SUBJECT","NOOP"}) {
        var record=record("_schemas","{\"keytype\":\""+type+"\"}",type.equals("NOOP")?null:new byte[]{0,1,(byte)255});
        assertSame(record,transform.apply(record));
      }
    }
  }
  @Test void leavesApplicationAndOtherBinaryRecordsUnchanged() {
    try(var transform = transform()) {
      var app=record("appTopic","{\"keytype\":\"MODE\"}",new byte[]{1});assertSame(app,transform.apply(app));
      var binary=record("binary-schemas","not-json",new byte[]{1});assertSame(binary,transform.apply(binary));
    }
  }
  @Test void escapedSubjectTextCannotBeMistakenForModeKeytype() {
    try(var transform = transform()) {
      var schema=record("_schemas","{\"keytype\":\"SCHEMA\",\"subject\":\"\\\"keytype\\\":\\\"MODE\\\"\"}",new byte[]{1});
      assertSame(schema,transform.apply(schema));
    }
  }
}

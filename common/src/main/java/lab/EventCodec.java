package lab;

import com.fasterxml.jackson.databind.JsonNode;
import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.util.*;
import org.apache.avro.Schema;
import org.apache.avro.generic.*;
import org.apache.avro.io.*;

/** Confluent magic-byte + 32-bit schema ID + Avro binary data; also reads legacy lab JSON. */
public final class EventCodec {
  public static final List<String> TYPES = List.of("OrderCreated", "PaymentReceived", "ShipmentDispatched", "InventoryUpdated", "CustomerUpdated");
  private static final List<String> DETAIL_FIELDS = List.of("orderId", "paymentId", "shipmentId", "sku", "customerId");
  private final RegistryClient registry;
  private final Map<Integer, Schema> schemas = new HashMap<>();
  private final List<Integer> producerIds = new ArrayList<>();

  public EventCodec(String url) { registry = new RegistryClient(url); }

  public static Schema schema(int index) {
    String type = TYPES.get(index);
    return new Schema.Parser().parse("""
        {"type":"record","name":"%s","namespace":"lab.events","fields":[
          {"name":"id","type":"string"},
          {"name":"seqNo","type":"long"},
          {"name":"createdAt","type":"string"},
          {"name":"eventType","type":"string"},
          {"name":"%s","type":"string"}
        ]}
        """.formatted(type, DETAIL_FIELDS.get(index)));
  }

  public void registerFive() throws Exception {
    producerIds.clear();
    for (int i = 0; i < TYPES.size(); i++) {
      Schema schema = schema(i);
      int id = registry.register("appTopic-" + schema.getFullName(), schema.toString());
      schemas.put(id, schema);
      producerIds.add(id);
    }
  }

  public Map<String, Integer> registered() {
    Map<String, Integer> result = new LinkedHashMap<>();
    for (int i = 0; i < producerIds.size(); i++) result.put("appTopic-lab.events." + TYPES.get(i), producerIds.get(i));
    return result;
  }

  public byte[] encode(String id, long seqNo, String createdAt) throws Exception {
    int index = (int) ((seqNo - 1) % TYPES.size());
    int schemaId = producerIds.get(index);
    var record = new GenericData.Record(schemas.get(schemaId));
    record.put("id", id);
    record.put("seqNo", seqNo);
    record.put("createdAt", createdAt);
    record.put("eventType", TYPES.get(index));
    record.put(DETAIL_FIELDS.get(index), "synthetic-" + seqNo);
    return encodeRecord(schemaId, record);
  }

  public static byte[] encodeRecord(int schemaId, GenericRecord record) throws Exception {
    var out = new ByteArrayOutputStream();
    out.write(ByteBuffer.allocate(5).put((byte) 0).putInt(schemaId).array());
    var encoder = EncoderFactory.get().binaryEncoder(out, null);
    new GenericDatumWriter<GenericRecord>(record.getSchema()).write(record, encoder);
    encoder.flush();
    return out.toByteArray();
  }

  public JsonNode decode(byte[] value) throws Exception {
    if (value == null) throw new IllegalArgumentException("Unexpected event tombstone");
    if (value.length == 0 || value[0] != 0) return DurableJson.JSON.readTree(value);
    if (value.length < 5) throw new IllegalArgumentException("Truncated Confluent frame");
    int id = ByteBuffer.wrap(value, 1, 4).getInt();
    Schema schema = schemas.get(id);
    if (schema == null) {
      schema = new Schema.Parser().parse(registry.schema(id));
      schemas.put(id, schema);
    }
    var decoder = DecoderFactory.get().binaryDecoder(value, 5, value.length - 5, null);
    var record = new GenericDatumReader<GenericRecord>(schema).read(null, decoder);
    return DurableJson.JSON.readTree(record.toString());
  }
}

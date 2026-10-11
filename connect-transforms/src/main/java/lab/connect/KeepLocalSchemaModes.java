package lab.connect;

import java.util.Map;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.transforms.Transformation;

/** MODE records are site policy; all other schema-store records remain byte-exact. */
public final class KeepLocalSchemaModes<R extends ConnectRecord<R>> implements Transformation<R> {
  private final JsonConverter json = new JsonConverter();
  @Override public void configure(Map<String, ?> configs) {
    json.configure(Map.of("schemas.enable", false), true);
  }
  @Override public R apply(R record) {
    if (!record.topic().contains("schemas") || !(record.key() instanceof byte[] key)) return record;
    try {
      Object decoded = json.toConnectData(record.topic(), key).value();
      if (decoded instanceof Map<?, ?> map && "MODE".equals(map.get("keytype"))) return null;
    } catch (DataException notJson) {
      // Topic-name matching may include other binary topics; never alter their data.
    }
    return record;
  }
  @Override public ConfigDef config() { return new ConfigDef(); }
  @Override public void close() { json.close(); }
}

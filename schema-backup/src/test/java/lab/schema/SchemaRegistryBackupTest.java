package lab.schema;

import static org.junit.jupiter.api.Assertions.*;
import com.sun.net.httpserver.HttpServer;
import java.net.InetSocketAddress;
import java.util.*;
import lab.DurableJson;
import org.junit.jupiter.api.*;

class SchemaRegistryBackupTest {
  private HttpServer server;
  private final Map<String,String> modes = new HashMap<>();
  private final List<String> writes = new ArrayList<>();
  @AfterEach void close() { if (server != null) server.stop(0); }
  private SchemaRegistryBackup guard() throws Exception {
    server = HttpServer.create(new InetSocketAddress("127.0.0.1",0),0);
    server.createContext("/mode", exchange -> {
      String path = exchange.getRequestURI().getPath();
      if (exchange.getRequestMethod().equals("PUT")) {
        String mode = DurableJson.JSON.readTree(exchange.getRequestBody()).path("mode").asText();
        modes.put(path,mode); writes.add(path + "=" + mode + (exchange.getRequestURI().getQuery()==null?"":"?"+exchange.getRequestURI().getQuery()));
      }
      byte[] data=DurableJson.JSON.writeValueAsBytes(Map.of("mode",modes.get(path)));
      exchange.sendResponseHeaders(200,data.length);exchange.getResponseBody().write(data);exchange.close();
    });server.start();return new SchemaRegistryBackup("http://127.0.0.1:"+server.getAddress().getPort());
  }
  private SchemaLogBackup.Snapshot snapshot(String scopedMode) {
    var records=new ArrayList<SchemaLogBackup.Entry>();
    if(scopedMode != null) records.add(new SchemaLogBackup.Entry(0,0,
        SchemaLogBackup.encode("{\"keytype\":\"MODE\",\"subject\":\"future-subject\"}".getBytes(java.nio.charset.StandardCharsets.UTF_8)),
        SchemaLogBackup.encode(("{\"mode\":\""+scopedMode+"\"}").getBytes(java.nio.charset.StandardCharsets.UTF_8)),List.of()));
    return new SchemaLogBackup.Snapshot(1,"now","source","_schemas",0,records.size(),records);
  }
  @Test void freezesGlobalAndOverridesThenRestoresReadwrite()throws Exception {
    modes.put("/mode","READWRITE");modes.put("/mode/future-subject","READWRITE");
    guard().withReadonly(snapshot("READWRITE"),()->assertTrue(modes.values().stream().allMatch("READONLY"::equals)));
    assertTrue(modes.values().stream().allMatch("READWRITE"::equals));
    assertEquals(List.of("/mode=READONLY","/mode/future-subject=READONLY","/mode/future-subject=READWRITE","/mode=READWRITE"),writes);
  }
  @Test void restoresSourceModeEvenWhenBackupFails()throws Exception {
    modes.put("/mode","READWRITE");var error=new IllegalStateException("capture failed");
    assertSame(error,assertThrows(IllegalStateException.class,()->guard().withReadonly(snapshot(null),()->{throw error;})));
    assertEquals("READWRITE",modes.get("/mode"));
  }
  @Test void preservesIntentionallyReadonlySourceWithoutModeWrites()throws Exception {
    modes.put("/mode","READONLY");guard().withReadonly(snapshot(null),()->assertEquals("READONLY",modes.get("/mode")));
    assertTrue(writes.isEmpty());
  }
  @Test void restoresImportOverrideUsingForce()throws Exception {
    modes.put("/mode","READWRITE");modes.put("/mode/future-subject","IMPORT");
    guard().withReadonly(snapshot("IMPORT"),()->assertEquals("READONLY",modes.get("/mode/future-subject")));
    assertTrue(writes.contains("/mode/future-subject=IMPORT?force=true"));
  }
  @Test void rejectsMismatchedUrlOrChangedOverridesBeforeWrites()throws Exception {
    modes.put("/mode","READWRITE");modes.put("/mode/future-subject","IMPORT");
    assertThrows(IllegalStateException.class,()->guard().withReadonly(snapshot("READWRITE"),()->fail("Must not capture")));
    assertTrue(writes.isEmpty());
  }
}

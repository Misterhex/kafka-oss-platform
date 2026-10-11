package lab.schema;

import static org.junit.jupiter.api.Assertions.*;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.sun.net.httpserver.HttpServer;
import java.net.InetSocketAddress;
import java.nio.file.*;
import java.util.*;
import lab.DurableJson;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.boot.DefaultApplicationArguments;

class SchemaApiRestoreTest {
  @TempDir Path directory;
  private HttpServer server;
  private final List<String> calls = new ArrayList<>();
  private final Map<String,ObjectNode> schemas = new HashMap<>();
  private String mode = "READWRITE";
  private boolean occupied, collision;
  @AfterEach void stop() { if(server != null) server.stop(0); }
  private SchemaLogBackup.Entry record(int offset, String key, String value) {
    return new SchemaLogBackup.Entry(offset,1234,SchemaLogBackup.encode(key.getBytes(java.nio.charset.StandardCharsets.UTF_8)),
        value==null?null:SchemaLogBackup.encode(value.getBytes(java.nio.charset.StandardCharsets.UTF_8)),List.of());
  }
  private SchemaLogBackup.Entry schema(int offset,String subject,int id,String refs,boolean deleted) {
    return record(offset,"{\"keytype\":\"SCHEMA\",\"subject\":\""+subject+"\",\"version\":1}",
        "{\"subject\":\""+subject+"\",\"id\":"+id+",\"version\":1,\"schemaType\":\"AVRO\",\"schema\":\"\\\"string\\\"\",\"deleted\":"+deleted+(refs==null?"":",\"references\":"+refs)+"}");
  }
  private SchemaLogBackup.Snapshot snapshot(List<SchemaLogBackup.Entry> entries) {
    return new SchemaLogBackup.Snapshot(1,"now","source","_schemas",0,100,entries);
  }
  private Path file(List<SchemaLogBackup.Entry> entries)throws Exception {
    var payload=DurableJson.JSON.valueToTree(snapshot(entries));var envelope=DurableJson.JSON.createObjectNode();
    envelope.put("format","confluent-schema-registry-kafka-log-v1");envelope.put("sha256",SchemaLogBackup.digest(payload));envelope.set("snapshot",payload);
    Path file=directory.resolve("backup.json");DurableJson.write(file,envelope);return file;
  }
  private String endpoint()throws Exception {
    server=HttpServer.create(new InetSocketAddress("127.0.0.1",0),0);
    server.createContext("/",exchange->{
      String method=exchange.getRequestMethod(),path=exchange.getRequestURI().getPath();calls.add(method+" "+exchange.getRequestURI());
      int status=200;Object response;
      if(path.equals("/contexts"))response=List.of(".");
      else if(path.equals("/subjects"))response=occupied?List.of("existing"):new ArrayList<>(new TreeSet<>(schemas.keySet()));
      else if(path.equals("/schemas"))response=List.of();
      else if(path.equals("/mode")){
        if(method.equals("PUT"))mode=DurableJson.JSON.readTree(exchange.getRequestBody()).path("mode").asText();
        response=Map.of("mode",mode);
      }else if(path.startsWith("/subjects/")){
        String subject=path.split("/")[2];
        if(method.equals("POST")){
          var body=(ObjectNode)DurableJson.JSON.readTree(exchange.getRequestBody());body.put("subject",subject);body.put("deleted",false);schemas.put(subject,body);
          if(collision){status=422;response=Map.of("error_code",42207,"message","Schema ID collision");}else response=Map.of("id",body.path("id").asInt());
        }else if(method.equals("DELETE")){schemas.get(subject).put("deleted",true);response=1;}
        else response=schemas.get(subject);
      }else{status=404;response=Map.of("error_code",40401);}
      byte[] bytes=DurableJson.JSON.writeValueAsBytes(response);exchange.sendResponseHeaders(status,bytes.length);exchange.getResponseBody().write(bytes);exchange.close();
    });server.start();return "http://127.0.0.1:"+server.getAddress().getPort();
  }
  @Test void ordersReferencesBeforeDependentsAndDropsTombstonedHistory()throws Exception {
    var entries=List.of(schema(0,"a-parent",2,"[{\"name\":\"Base\",\"subject\":\"z-base\",\"version\":1}]",false),
        schema(1,"z-base",1,null,false),schema(2,"removed",3,null,false),record(3,"{\"keytype\":\"SCHEMA\",\"subject\":\"removed\",\"version\":1}",null),record(4,"{\"keytype\":\"NOOP\"}",null));
    var plan=SchemaApiRestore.plan(snapshot(entries));
    assertEquals(List.of("z-base","a-parent"),plan.schemas().stream().map(SchemaApiRestore.Schema::subject).toList());
  }
  @Test void rejectsMissingReferencesContextsAndUnsupportedStateBeforeMutation()throws Exception {
    assertThrows(IllegalArgumentException.class,()->SchemaApiRestore.plan(snapshot(List.of(schema(0,"a",1,"[{\"subject\":\"absent\",\"version\":1}]",false)))));
    assertThrows(IllegalArgumentException.class,()->SchemaApiRestore.plan(snapshot(List.of(schema(0,":.audit:a",1,null,false)))));
    var backup=file(List.of(record(0,"{\"keytype\":\"EXPORTER\"}","{}")));
    assertThrows(IllegalArgumentException.class,()->new SchemaApiRestore(endpoint()).restore(backup));assertTrue(calls.isEmpty());
  }
  @Test void restoresExplicitIdsAndSoftDeletionThenReturnsToOriginalMode()throws Exception {
    new SchemaApiRestore(endpoint()).restore(file(List.of(schema(0,"example",24,null,true))));
    assertEquals(24,schemas.get("example").path("id").asInt());assertTrue(schemas.get("example").path("deleted").asBoolean());
    assertEquals("READWRITE",mode);
    assertTrue(calls.indexOf("POST /subjects/example/versions")<calls.indexOf("DELETE /subjects/example/versions/1"));
    assertTrue(calls.contains("GET /subjects/example/versions/1?deleted=true"));
  }
  @Test void refusesPopulatedRegistryBeforeAnyWrites()throws Exception {
    occupied=true;
    assertThrows(IllegalStateException.class,()->new SchemaApiRestore(endpoint()).restore(file(List.of(schema(0,"a",1,null,false)))));
    assertTrue(calls.stream().allMatch(call->call.startsWith("GET ")));assertEquals("READWRITE",mode);
  }
  @Test void collisionFailsAndLeavesDestinationInImportMode()throws Exception {
    collision=true;
    var failure=assertThrows(IllegalStateException.class,()->new SchemaApiRestore(endpoint()).restore(file(List.of(schema(0,"a",1,null,false)))));
    assertTrue(failure.getCause().getMessage().contains("collision"));assertEquals("IMPORT",mode);
  }
  @Test void rejectsKafkaOptionsOnImportPath() {
    assertThrows(IllegalArgumentException.class,()->new SchemaBackupApplication().run(new DefaultApplicationArguments("restore","--via-import","--registry-url=http://localhost:1","--file=backup.json","--registry-stopped")));
  }
  @Test void importAllowsExplicitChecksumOverrideButStrictDefaultDoesNotWrite()throws Exception {
    Path backup=file(List.of(schema(0,"override",24,null,false)));
    var envelope=(ObjectNode)DurableJson.JSON.readTree(backup.toFile());envelope.put("sha256","edited-checksum");DurableJson.write(backup,envelope);
    var restore=new SchemaApiRestore(endpoint());
    assertThrows(IllegalArgumentException.class,()->restore.restore(backup));assertTrue(calls.isEmpty());
    restore.restore(backup,true);assertEquals(24,schemas.get("override").path("id").asInt());assertEquals("READWRITE",mode);
  }
  @Test void checksumOverrideIsABooleanFlagAndNotAvailableForBackup() {
    assertThrows(IllegalArgumentException.class,()->new SchemaBackupApplication().run(new DefaultApplicationArguments("restore","--via-import","--registry-url=http://localhost:1","--file=backup.json","--allow-checksum-mismatch=false")));
    assertThrows(IllegalArgumentException.class,()->new SchemaBackupApplication().run(new DefaultApplicationArguments("backup","--bootstrap=localhost:1","--file=backup.json","--allow-checksum-mismatch")));
  }
  @Test void subjectDeletionMarkersAreAppliedToRetainedVersions()throws Exception {
    var plan=SchemaApiRestore.plan(snapshot(List.of(schema(0,"deleted",7,null,false),record(1,"{\"keytype\":\"DELETE_SUBJECT\",\"subject\":\"deleted\"}","{\"subject\":\"deleted\",\"version\":1}"))));
    assertTrue(plan.schemas().getFirst().deleted());
  }
  @Test void conflictingBackupIdsAreRejectedBeforeAnyApiCalls()throws Exception {
    var second=schema(1,"b",1,null,false);
    String body=new String(SchemaLogBackup.decode(second.value()),java.nio.charset.StandardCharsets.UTF_8).replace("string","int");
    assertThrows(IllegalArgumentException.class,()->SchemaApiRestore.plan(snapshot(List.of(schema(0,"a",1,null,false),record(1,"{\"keytype\":\"SCHEMA\",\"subject\":\"b\",\"version\":1}",body)))));
  }
}

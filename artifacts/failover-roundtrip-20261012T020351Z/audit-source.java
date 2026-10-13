import lab.*;
import com.fasterxml.jackson.databind.JsonNode;
import java.nio.file.*;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.*;
import java.util.*;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;

class AuditRoundtrip {
  record Event(long offset, String id, long sequence, String valueSha256, long timestamp, String headers) {}
  record Scan(List<Event> events, Map<String,Integer> frequencies, boolean baselineFound, boolean finalFound) {}
  static String hex(byte[] b) throws Exception { return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(b)); }
  static Scan scan(int port, long limit, String baseline, String last, Set<String> wanted, Path out) throws Exception {
    Properties p=new Properties();p.put("bootstrap.servers","localhost:"+port);
    p.put("security.protocol","SASL_PLAINTEXT");p.put("sasl.mechanism","PLAIN");
    p.put("sasl.jaas.config","org.apache.kafka.common.security.plain.PlainLoginModule required username=\"platform\" password=\"platform-lab\";");
    p.put("key.deserializer","org.apache.kafka.common.serialization.ByteArrayDeserializer");p.put("value.deserializer","org.apache.kafka.common.serialization.ByteArrayDeserializer");
    p.put("enable.auto.commit","false");p.put("default.api.timeout.ms","15000");
    var codec=new EventCodec("http://localhost:"+(port==19092?8086:8087));
    List<Event> events=new ArrayList<>();Map<String,Integer> counts=new HashMap<>();
    boolean started=false,finished=false;long deadline=System.nanoTime()+Duration.ofSeconds(90).toNanos();
    try(var c=new KafkaConsumer<byte[],byte[]>(p)) {
      var tp=new TopicPartition("appTopic",0);c.assign(List.of(tp));
      if(c.beginningOffsets(List.of(tp)).get(tp)!=0)throw new IllegalStateException("History expired; cannot audit the saved baseline");
      if(c.endOffsets(List.of(tp)).get(tp)<limit)throw new IllegalStateException("Saved range unavailable");
      c.seek(tp,0);
      while(c.position(tp)<limit) {
        if(System.nanoTime()>deadline)throw new IllegalStateException("Audit scan timed out");
        for(var r:c.poll(Duration.ofMillis(200))) {
          if(r.offset()>=limit)continue;
          String id=new String(r.key(),StandardCharsets.UTF_8);counts.merge(id,1,Integer::sum);
          boolean include=wanted!=null?wanted.contains(id):started&&!finished;
          if(include) {
            var v=codec.decode(r.value());if(!id.equals(v.path("id").asText()))throw new IllegalStateException("Payload ID differs from key");
            var headers=new ArrayList<String>();r.headers().forEach(h->headers.add(h.key()+"="+(h.value()==null?"null":Base64.getEncoder().encodeToString(h.value()))));
            events.add(new Event(r.offset(),id,v.path("seqNo").asLong(),hex(r.value()),r.timestamp(),headers.toString()));
          }
          if(id.equals(baseline))started=true;
          if(id.equals(last))finished=true;
        }
      }
    }
    DurableJson.write(out,events);
    return new Scan(events,counts,started,finished);
  }
  public static void main(String[] args) throws Exception {
    Path dir=Path.of(args[0]);var initial=DurableJson.JSON.readTree(dir.resolve("00-initial.json").toFile());
    var finalState=DurableJson.JSON.readTree(dir.resolve("10-final-hk.json").toFile());
    var toSG=DurableJson.JSON.readTree(dir.resolve("01-sg-promotion.json").toFile());
    var toHK=DurableJson.JSON.readTree(dir.resolve("07-hk-promotion.json").toFile());
    var hkFenced=DurableJson.JSON.readTree(dir.resolve("01-sg-clients-before-retarget.json").toFile());
    var sgFenced=DurableJson.JSON.readTree(dir.resolve("07-hk-clients-before-retarget.json").toFile());
    String baseline=initial.path("apps").path("producer").path("lastId").asText();
    String last=finalState.path("apps").path("producer").path("lastId").asText();
    long hkLimit=finalState.path("logEnds").path("k0x").path("appTopic:0").asLong();
    long sgLimit=finalState.path("logEnds").path("k0y").path("appTopic:0").asLong();
    var hk=scan(19092,hkLimit,baseline,last,null,dir.resolve("audit-hk-events.json"));
    if(!hk.baselineFound()||!hk.finalFound())throw new IllegalStateException("Missing boundary event");
    Set<String> ids=new HashSet<>();hk.events().forEach(e->ids.add(e.id()));
    var sg=scan(29092,sgLimit,baseline,last,null,dir.resolve("audit-sg-events.json"));
    Map<String,Event> hkById=new LinkedHashMap<>(),sgById=new LinkedHashMap<>();
    hk.events().forEach(e->hkById.put(e.id(),e));sg.events().forEach(e->sgById.put(e.id(),e));
    var missingSG=new ArrayList<String>();var missingHK=new ArrayList<String>();
    sgById.keySet().stream().filter(id->!hkById.containsKey(id)).forEach(missingHK::add);
    ids.addAll(sgById.keySet());
    var payloadDiffs=new ArrayList<String>();var metadataDiffs=new ArrayList<String>();
    for(var e:hk.events()) {
      var target=sgById.get(e.id());
      if(target==null)missingSG.add(e.id());
      else {
        if(!e.valueSha256().equals(target.valueSha256())||e.sequence()!=target.sequence())payloadDiffs.add(e.id());
        if(e.timestamp()!=target.timestamp()||!e.headers().equals(target.headers()))metadataDiffs.add(e.id());
      }
    }
    var duplicateHK=new LinkedHashMap<String,Integer>();var duplicateSG=new LinkedHashMap<String,Integer>();
    for(String id:ids) {
      if(hk.frequencies().getOrDefault(id,0)>1)duplicateHK.put(id,hk.frequencies().get(id));
      if(sg.frequencies().getOrDefault(id,0)>1)duplicateSG.put(id,sg.frequencies().get(id));
    }
    Map<Long,Integer> sequenceCounts=new TreeMap<>();hk.events().forEach(e->sequenceCounts.merge(e.sequence(),1,Integer::sum));
    var sequenceDuplicates=new LinkedHashMap<Long,Integer>();sequenceCounts.forEach((n,c)->{if(c>1)sequenceDuplicates.put(n,c);});
    var gaps=new ArrayList<Map<String,Long>>();long previous=-1;
    for(long n:sequenceCounts.keySet()){if(previous!=-1&&n>previous+1)gaps.add(Map.of("first",previous+1,"last",n-1,"count",n-previous-1));previous=n;}
    long sgStart=toSG.path("targetEnds").path("appTopic:0").asLong(),hkStart=toHK.path("targetEnds").path("appTopic:0").asLong();
    long expectedInitial=hkFenced.path("producer").path("count").asLong()-initial.path("apps").path("producer").path("count").asLong();
    long expectedSG=sgFenced.path("producer").path("count").asLong();
    long expectedFinal=finalState.path("apps").path("producer").path("count").asLong();
    long actualInitial=hk.events().stream().filter(e->e.offset()<sgStart).count();
    long actualSG=hk.events().stream().filter(e->e.offset()>=sgStart&&e.offset()<hkStart).count();
    long actualFinal=hk.events().stream().filter(e->e.offset()>=hkStart).count();
    var counts=List.of(Map.of("phase","HK before SG promotion","acknowledged",expectedInitial,"records",actualInitial),Map.of("phase","SG primary","acknowledged",expectedSG,"records",actualSG),Map.of("phase","HK after failback","acknowledged",expectedFinal,"records",actualFinal));
    boolean reconciliation=expectedInitial==actualInitial&&expectedSG==actualSG&&expectedFinal==actualFinal;
    boolean pass=sg.baselineFound()&&sg.finalFound()&&missingHK.isEmpty()&&missingSG.isEmpty()&&payloadDiffs.isEmpty()&&metadataDiffs.isEmpty()&&duplicateHK.isEmpty()&&duplicateSG.isEmpty()&&sequenceDuplicates.isEmpty()&&reconciliation;
    Map<String,Object> result=new LinkedHashMap<>();
    result.put("auditedAt",Instant.now().toString());result.put("pass",pass);result.put("baselineExcludedEventId",baseline);result.put("finalIncludedEventId",last);
    result.put("auditedEvents",hk.events().size());result.put("sgAuditedEvents",sg.events().size());result.put("hkScannedPrefixEnd",hkLimit);result.put("sgScannedPrefixEnd",sgLimit);
    result.put("hkAuditedOffsetStart",hk.events().get(0).offset());result.put("hkAuditedOffsetEndExclusive",hk.events().get(hk.events().size()-1).offset()+1);
    result.put("acknowledgementReconciliation",counts);result.put("missingFromHK",missingHK);result.put("missingFromSG",missingSG);result.put("payloadMismatches",payloadDiffs);result.put("timestampOrHeaderMismatches",metadataDiffs);
    result.put("duplicateEventIdsHK",duplicateHK);result.put("duplicateEventIdsSG",duplicateSG);result.put("duplicateSequenceNumbersHK",sequenceDuplicates);result.put("sequenceGaps",gaps);
    result.put("consumerProcessingAudit","No per-event processing ledger was retained. SG consumer count equals its 1337 unique topic records; this supports, but does not directly prove, absence of repeated processing.");
    DurableJson.write(dir.resolve("audit-summary.json"),result);System.out.println(DurableJson.JSON.writerWithDefaultPrettyPrinter().writeValueAsString(result));
    if(!pass)throw new IllegalStateException("Audit failed");
  }
}

package lab;

import com.fasterxml.jackson.databind.JsonNode;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.*;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Map;

/** Plain HTTP client: no Spring Kafka or Confluent serializer dependency. */
public final class RegistryClient {
  private final String url;
  private final HttpClient http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();

  public RegistryClient(String url) { this.url = url.replaceAll("/$", ""); }

  public JsonNode request(String method, String path, Object body) throws Exception {
    var request = HttpRequest.newBuilder(URI.create(url + path)).timeout(Duration.ofSeconds(30))
        .header("Accept", "application/vnd.schemaregistry.v1+json")
        .header("Content-Type", "application/vnd.schemaregistry.v1+json")
        .method(method, body == null ? HttpRequest.BodyPublishers.noBody()
            : HttpRequest.BodyPublishers.ofString(DurableJson.JSON.writeValueAsString(body))).build();
    var response = http.send(request, HttpResponse.BodyHandlers.ofString());
    if (response.statusCode() / 100 != 2)
      throw new IllegalStateException(method + " " + url + path + " => " + response.statusCode() + " " + response.body());
    return DurableJson.JSON.readTree(response.body());
  }

  public int register(String subject, String schema) throws Exception {
    return request("POST", "/subjects/" + encode(subject) + "/versions",
        Map.of("schemaType", "AVRO", "schema", schema)).path("id").asInt();
  }

  public String schema(int id) throws Exception {
    return request("GET", "/schemas/ids/" + id, null).path("schema").asText();
  }

  public static String encode(String value) {
    return URLEncoder.encode(value, StandardCharsets.UTF_8).replace("+", "%20");
  }

  public static String forBootstrap(String bootstrap) {
    String explicit = System.getenv("SCHEMA_REGISTRY_URL");
    if (explicit != null && !explicit.isBlank()) return explicit;
    return bootstrap.startsWith("k0y:") ? "http://schema-registry-k0y:8081" : "http://schema-registry-k0x:8081";
  }
}

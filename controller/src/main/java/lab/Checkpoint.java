package lab;

import java.util.*;

/** Last-record identities used to verify replication, not consumer restart positions. */
@com.fasterxml.jackson.annotation.JsonIgnoreProperties(ignoreUnknown = true)
public class Checkpoint {
  public String capturedAt = "";
  public Map<Integer, String> barriers = new TreeMap<>();
}

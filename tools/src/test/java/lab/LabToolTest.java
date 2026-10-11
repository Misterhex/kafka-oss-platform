package lab;

import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.databind.JsonNode;
import org.junit.jupiter.api.Test;

class LabToolTest {
  @Test
  void seedsLastProcessedOffsetsAndResetsAnEmptyPartition() {
    JsonNode result =
        DurableJson.JSON.valueToTree(LabTool.reverseOffsets("appTopic:0:120\n", "sg"));
    assertEquals(119, result.at("/offsets/0/offset/offset").asLong());
    assertEquals("sg", result.at("/offsets/0/partition/cluster").asText());
    result = DurableJson.JSON.valueToTree(LabTool.reverseOffsets("appTopic:0:1\n", "sg"));
    assertEquals(0, result.at("/offsets/0/offset/offset").asLong());
    result = DurableJson.JSON.valueToTree(LabTool.reverseOffsets("appTopic:0:0\n", "sg"));
    assertTrue(result.at("/offsets/0/offset").isNull());
  }

  @Test
  void rejectsIncompleteOrDuplicateBoundaries() {
    assertThrows(IllegalStateException.class, () -> LabTool.reverseOffsets("", "sg"));
    assertThrows(
        IllegalStateException.class,
        () -> LabTool.reverseOffsets("appTopic:0:1\nappTopic:0:2\n", "sg"));
  }
}

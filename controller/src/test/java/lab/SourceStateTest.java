package lab;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;
import java.util.*;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;

class SourceStateTest {
  @Test void capturesAllGroupsAndPartitionsWithoutApplyingOffsets() throws Exception {
    var admin = mock(Admin.class);
    var listings = mock(ListConsumerGroupsResult.class);
    var first = mock(ConsumerGroupListing.class);
    var second = mock(ConsumerGroupListing.class);
    when(first.groupId()).thenReturn("orders");
    when(second.groupId()).thenReturn("payments");
    when(admin.listConsumerGroups()).thenReturn(listings);
    when(listings.all()).thenReturn(KafkaFuture.completedFuture(List.of(first, second)));
    var orders = mock(ListConsumerGroupOffsetsResult.class);
    var payments = mock(ListConsumerGroupOffsetsResult.class);
    when(admin.listConsumerGroupOffsets("orders")).thenReturn(orders);
    when(admin.listConsumerGroupOffsets("payments")).thenReturn(payments);
    when(orders.partitionsToOffsetAndMetadata()).thenReturn(KafkaFuture.completedFuture(Map.of(
        new TopicPartition("appTopic", 0), new OffsetAndMetadata(90),
        new TopicPartition("anotherTopic", 1), new OffsetAndMetadata(231))));
    when(payments.partitionsToOffsetAndMetadata()).thenReturn(KafkaFuture.completedFuture(Map.of(
        new TopicPartition("payments", 2), new OffsetAndMetadata(17))));
    var captured = DrApplication.captureGroupOffsets(admin);
    assertEquals(Map.of("orders", Map.of("appTopic:0",90L,"anotherTopic:1",231L),
        "payments",Map.of("payments:2",17L)), captured);
    verify(admin, never()).alterConsumerGroupOffsets(anyString(), anyMap());
  }
}

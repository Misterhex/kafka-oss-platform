package lab;

import java.util.*;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.common.acl.*;
import org.apache.kafka.common.resource.*;

/** Application ACLs only. Platform and MirrorMaker use separate authenticated identities. */
final class PlatformAccess {
  static List<AclBinding> denies() {
    var bindings = new ArrayList<AclBinding>();
    for (String user : List.of("producer", "consumer"))
      bindings.add(binding(ResourceType.TOPIC, KafkaSupport.TOPIC, user, AclOperation.ALL, AclPermissionType.DENY));
    bindings.add(binding(ResourceType.GROUP, KafkaSupport.GROUP, "consumer", AclOperation.ALL, AclPermissionType.DENY));
    return bindings;
  }

  static List<AclBinding> allows() {
    return List.of(
        binding(ResourceType.TOPIC, KafkaSupport.TOPIC, "producer", AclOperation.WRITE, AclPermissionType.ALLOW),
        binding(ResourceType.TOPIC, KafkaSupport.TOPIC, "producer", AclOperation.DESCRIBE, AclPermissionType.ALLOW),
        binding(ResourceType.CLUSTER, Resource.CLUSTER_NAME, "producer", AclOperation.IDEMPOTENT_WRITE, AclPermissionType.ALLOW),
        binding(ResourceType.TOPIC, KafkaSupport.TOPIC, "consumer", AclOperation.READ, AclPermissionType.ALLOW),
        binding(ResourceType.TOPIC, KafkaSupport.TOPIC, "consumer", AclOperation.DESCRIBE, AclPermissionType.ALLOW),
        binding(ResourceType.GROUP, KafkaSupport.GROUP, "consumer", AclOperation.READ, AclPermissionType.ALLOW));
  }

  private static AclBinding binding(ResourceType type, String name, String user, AclOperation operation, AclPermissionType permission) {
    return new AclBinding(new ResourcePattern(type, name, PatternType.LITERAL),
        new AccessControlEntry("User:" + user, "*", operation, permission));
  }

  static void fence(String bootstrap) throws Exception {
    try (var admin = KafkaSupport.admin(bootstrap)) {
      admin.createAcls(denies()).all().get(15, TimeUnit.SECONDS);
      var actual = admin.describeAcls(AclBindingFilter.ANY).values().get(15, TimeUnit.SECONDS);
      if (!actual.containsAll(denies())) throw new IllegalStateException("Application fence not installed");
    }
  }

  static void enable(String bootstrap) throws Exception {
    try (var admin = KafkaSupport.admin(bootstrap)) {
      admin.createAcls(allows()).all().get(15, TimeUnit.SECONDS);
      admin.deleteAcls(denies().stream().map(AclBinding::toFilter).toList()).all().get(15, TimeUnit.SECONDS);
    }
  }
}

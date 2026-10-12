package lab.schema;

import java.nio.file.Path;
import java.util.*;
import org.springframework.boot.*;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/** One-shot Java CLI. Spring Boot supplies argument parsing and ApplicationRunner lifecycle. */
@SpringBootApplication
public class SchemaBackupApplication implements ApplicationRunner {
  public static void main(String[] args) {
    var app = new SpringApplication(SchemaBackupApplication.class);
    app.setWebApplicationType(WebApplicationType.NONE);
    app.setBannerMode(Banner.Mode.OFF);
    try (var context = app.run(args)) { }
  }

  @Override public void run(ApplicationArguments args) throws Exception {
    if (args.containsOption("help")) {
      System.out.println("""
          Schema Registry 8.1.1 backup CLI (Java 21)
          backup  --bootstrap=HOST:PORT --file=PATH [--topic=_schemas] [--overwrite]
                  [--kafka-properties=PATH]
          Backup reads Kafka only and preserves stored records without changing registry modes.
          inspect --file=PATH
          restore --bootstrap=HOST:PORT --file=PATH --registry-stopped
                  [--topic=_schemas] [--replication-factor=1] [--kafka-properties=PATH]
          restore --via-import --registry-url=http://HOST:PORT --file=PATH
          Both restore methods accept [--allow-checksum-mismatch] to bypass ONLY checksum
          mismatch rejection with a warning. All other validation remains enabled.
          IMPORT restore requires a RUNNING, empty destination registry, mode.mutability=true,
          and no other registry writers. Restores supported default-context schema/config/mode
          state through REST, not raw log history. Use Kafka replay for contexts/other key types.
          Backup requires no concurrent registry writes. Kafka replay requires an EMPTY one-partition
          target topic and ALL destination registry instances stopped. Start the destination
          with kafkastore.topic matching --topic after restoration. An interrupted restore leaves
          partial data: keep the registry stopped and use a fresh empty topic for the next attempt.
          """);
      return;
    }
    if (args.getNonOptionArgs().size() != 1) throw new IllegalArgumentException("Specify backup, inspect or restore; use --help");
    String command = args.getNonOptionArgs().getFirst();
    Set<String> allowed = switch (command) {
      case "backup" -> Set.of("bootstrap", "file", "topic", "overwrite", "kafka-properties");
      case "restore" -> Set.of("bootstrap", "file", "topic", "registry-stopped", "replication-factor", "kafka-properties", "via-import", "registry-url", "allow-checksum-mismatch");
      case "inspect" -> Set.of("file");
      default -> throw new IllegalArgumentException("Unknown command: " + command);
    };
    for (String option : args.getOptionNames())
      if (!allowed.contains(option)) throw new IllegalArgumentException("Unknown option: " + option);
    Path file = Path.of(required(args, "file")).toAbsolutePath();
    boolean allowChecksumMismatch = args.containsOption("allow-checksum-mismatch");
    if (allowChecksumMismatch && !args.getOptionValues("allow-checksum-mismatch").isEmpty())
      throw new IllegalArgumentException("--allow-checksum-mismatch is a flag; it takes no value");
    if (command.equals("inspect")) { SchemaLogBackup.inspect(file); return; }
    if (command.equals("restore") && args.containsOption("via-import")) {
      if (!args.getOptionValues("via-import").isEmpty()) throw new IllegalArgumentException("--via-import is a flag; it takes no value");
      for (String incompatible : List.of("bootstrap", "topic", "registry-stopped", "replication-factor", "kafka-properties"))
        if (args.containsOption(incompatible)) throw new IllegalArgumentException("--" + incompatible + " cannot be used with --via-import");
      new SchemaApiRestore(required(args, "registry-url")).restore(file, allowChecksumMismatch);
      return;
    }
    if (command.equals("restore") && args.containsOption("registry-url")) throw new IllegalArgumentException("--registry-url requires --via-import");
    String bootstrap = required(args, "bootstrap");
    String topic = option(args, "topic", "_schemas");
    Properties properties = new Properties();
    if (args.containsOption("kafka-properties"))
      try (var in = java.nio.file.Files.newInputStream(Path.of(required(args, "kafka-properties")))) { properties.load(in); }
    var service = new SchemaLogBackup(bootstrap, topic, properties);
    if (command.equals("backup")) {
      boolean overwrite = args.containsOption("overwrite");
      if (java.nio.file.Files.exists(file) && !overwrite) throw new IllegalArgumentException("Backup already exists; use another file or --overwrite");
      service.backup(file, overwrite);
    }
    else {
      if (!args.containsOption("registry-stopped"))
        throw new IllegalArgumentException("Stop ALL destination Schema Registry instances, then pass --registry-stopped");
      service.restore(file, Short.parseShort(option(args, "replication-factor", "1")), allowChecksumMismatch);
    }
  }

  private static String required(ApplicationArguments args, String name) {
    String value = option(args, name, "");
    if (value.isBlank()) throw new IllegalArgumentException("Missing --" + name);
    return value;
  }
  private static String option(ApplicationArguments args, String name, String fallback) {
    var values = args.getOptionValues(name);
    if (values == null) return fallback;
    if (values.size() != 1) throw new IllegalArgumentException("--" + name + " requires exactly one value");
    return values.getFirst();
  }
}

package lab;

import static java.nio.file.StandardOpenOption.*;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.*;

public final class DurableJson {
  public static final ObjectMapper JSON = new ObjectMapper();

  public static synchronized void write(Path path, Object data) throws Exception {
    Files.createDirectories(path.getParent());
    Path tmp = path.resolveSibling(path.getFileName() + ".tmp");
    byte[] bytes = JSON.writerWithDefaultPrettyPrinter().writeValueAsBytes(data);
    try (var f = FileChannel.open(tmp, CREATE, TRUNCATE_EXISTING, WRITE)) {
      var b = ByteBuffer.wrap(bytes);
      while (b.hasRemaining()) f.write(b);
      f.force(true);
    }
    Files.move(tmp, path, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
    try (var directory = FileChannel.open(path.getParent(), READ)) {
      directory.force(true);
    }
  }

  private DurableJson() {}
}

package lab;

/** A deliberately sequential workflow. An external workflow engine can wrap these stages later. */
public final class Promotion {
  public interface Stages {
    void drain(String source, String target) throws Exception;
    void sync(String source, String target) throws Exception;
    void flip(String source, String target) throws Exception;
  }

  public static void promote(String current, String target, Stages stages) throws Exception {
    validate(current, target);
    stages.drain(current, target);
    stages.sync(current, target);
    stages.flip(current, target);
  }

  static void validate(String current, String target) {
    if (!java.util.Set.of("k0x", "k0y").contains(current)) throw new IllegalArgumentException("Unknown current primary");
    if (!java.util.Set.of("k0x", "k0y").contains(target)) throw new IllegalArgumentException("Target must be k0x or k0y");
    if (current.equals(target)) throw new IllegalArgumentException("Target is already primary");
  }

  private Promotion() {}
}

package io.wzp.index.benchmark;

import java.util.List;

/**
 * @author wzp
 * @since 2019/06/16
 */

public class BenchmarkEvent {
  private final List<byte[]> key;
  private final List<byte[]> result;

  public BenchmarkEvent(List<byte[]> key, List<byte[]> result) {
    this.key = key;
    this.result = result;
  }

  public List<byte[]> getKeys() {
    return key;
  }

  public List<byte[]> getResult() {
    return result;
  }
}

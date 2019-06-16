package io.wzp.index.core;

import java.util.HashMap;
import java.util.Map;
import java.util.NoSuchElementException;

/**
 * @author wzp
 * @since 2019/06/15
 */

public class Conf {

  private final Map<String, String> vars = new HashMap<>();

  public Conf() {
    for (ConfVar var : ConfVar.values()) {
      vars.put(var.varName, var.getDefaultValue());
    }
  }

  public enum ConfVar {
    SKIP_LIST_SIZE_LIMIT_MB("skiplist.size.limit.mb", "1024"),
    BLOCK_SIZE_LIMIT_MB("block.size.limit.mb", "16"),
    BLOCK_INDEX_SPAN("block.index.span", "1000"),
    SEGMENT_SIZE_LIMIT_MB("segment.size.limit.mb", "10240"),
    BLOCK_CACHE_SIZE_LIMIT_MB("block.cache.size.limit.mb", "1024"),
    BIG_VALUE_SIZE_LIMIT_KB("big.value.size.limit.kb", "4"),
    VALUE_FILE_SIZE_LIMIT_MB("value.file.size.limit.mb", "10240"),
    FLUSH_THREAD_NUM("flush.thread.num", "2");

    private final String varName;
    private final String defaultVal;

    ConfVar(String varName, String defaultVal) {
      this.varName = varName;
      this.defaultVal = defaultVal;
    }

    public String getName() {
      return varName;
    }

    public String getDefaultValue() {
      return defaultVal;
    }
  }

  public String get(ConfVar key) {
    if (vars.containsKey(key.getName())) {
      return vars.get(key.getName());
    } else {
      throw new NoSuchElementException(key.getName());
    }
  }

  public void set(ConfVar key, String value) {
    if (value == null) {
      throw new NullPointerException("Null value found");
    }

    vars.put(key.getName(), value);
  }
}

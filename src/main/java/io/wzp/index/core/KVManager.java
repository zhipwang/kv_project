package io.wzp.index.core;

import io.wzp.index.io.KVRecord;
import io.wzp.index.io.Segment;
import io.wzp.index.io.ValueFile;

import java.io.IOException;
import java.util.List;

/**
 * @author wzp
 * @since 2019/06/16
 */

public class KVManager {

  private final List<Segment> segments;
  private final List<ValueFile> valueFiles;

  public KVManager(List<Segment> segments, List<ValueFile> valueFiles) {
    this.segments = segments;
    this.valueFiles = valueFiles;
  }

  public byte[] getValue(byte[] key) throws IOException {
    KVRecord record = null;
    //Suppose no duplicate key
    for (Segment segment : segments) {
      record = segment.get(key);

      if (record != null) {
        break;
      }
    }

    if (record == null) {
      return null;
    }

    if (record.isValueWithKey()) {
      return record.getValue();
    } else {
      int fileId = record.getValueFileNum();

      if (fileId < 0 || fileId >= valueFiles.size()) {
        throw new IOException(String.format("Invalid file id (%d) with list size (%d)", fileId, valueFiles.size()));
      }

      return valueFiles.get(fileId).get(record.getValueFileOffset(), record.getValueLength());
    }
  }
}

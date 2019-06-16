package io.wzp.index.io;

import java.io.IOException;

/**
 * @author wzp
 * @since 2019/06/16
 */

public interface Segment {

  String getFilePath();

  int getSegmentId();

  void init() throws IOException;

  void close() throws IOException;

  void put(KVRecord record) throws IOException;

  void flush() throws IOException;

  boolean isFull() throws IOException;

  byte[] getStartKey();

  byte[] getEndKey();

  KVRecord get(byte[] targetKey) throws IOException;


}

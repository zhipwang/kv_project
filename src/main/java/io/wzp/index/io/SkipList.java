package io.wzp.index.io;

import java.util.Iterator;

/**
 * @author wzp
 * @since 2019/06/16
 */

public interface SkipList {

  boolean isFull();

  boolean isEmpty();

  void reset();

  void put(KVRecord record);

  KVRecord getFirstRecord();

  KVRecord getLastRecord();

  KVRecord get(byte[] key);

  Iterator<KVRecord> toIterator();
}

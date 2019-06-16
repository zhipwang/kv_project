package io.wzp.index.io;

/**
 * @author wzp
 * @since 2019/06/16
 */

public interface DataBlockCache {

  DataBlock get(long cacheId);

  void put(long cacheId, DataBlock block);
}

package io.wzp.index.io;

import io.wzp.index.core.Conf;
import io.wzp.index.utils.BufferHelper;

import java.util.HashMap;
import java.util.Map;

/**
 * @author wzp
 * @since 2019/06/16
 */

public class DataBlockCacheImpl implements DataBlockCache {

  private final Map<Long, CacheNode> hashMap;
  private final long cacheSizeLimit;
  private final CacheNodeList list;
  private long curSize;

  public DataBlockCacheImpl(Conf conf) {
    this.hashMap = new HashMap<>();
    this.cacheSizeLimit = 1024L * 1024L * Integer.parseInt(conf.get(Conf.ConfVar.BLOCK_CACHE_SIZE_LIMIT_MB));
    this.list = new CacheNodeList();
    this.curSize = 0;
  }

  @Override
  public synchronized DataBlock get(long cacheId) {
    CacheNode node = hashMap.get(cacheId);

    if (node == null) {
      return null;
    }

    list.remove(node);
    list.addFirst(node);

    return node.getBlock();
  }

  private void evict() {
    CacheNode node = list.removeLast();

    if (node != null) {
      hashMap.remove(node.getCacheId());

      curSize -= node.getBlock().getSize();

      BufferHelper.disposeMappedByteBuffer(node.getBlock().getBuffer());
    }
  }

  @Override
  public synchronized void put(long cacheId, DataBlock block) {
    if (!hashMap.containsKey(cacheId)) {
      if (curSize >= cacheSizeLimit) {
        evict();
      }

      CacheNode node = new CacheNode(cacheId, block);

      hashMap.put(cacheId, node);
      list.addFirst(node);
      curSize += block.getSize();
    }
  }
}

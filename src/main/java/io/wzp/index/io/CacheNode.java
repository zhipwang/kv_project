package io.wzp.index.io;

/**
 * @author wzp
 * @since 2019/06/16
 */

public class CacheNode {

  private final long cacheId;
  private final DataBlock block;

  private CacheNode prev;
  private CacheNode next;

  public CacheNode(long cacheId, DataBlock block) {
    this.cacheId = cacheId;
    this.block = block;
  }

  public long getCacheId() {
    return cacheId;
  }

  public DataBlock getBlock() {
    return block;
  }

  public CacheNode getPrev() {
    return prev;
  }

  public CacheNode getNext() {
    return next;
  }

  public void setPrev(CacheNode prev) {
    this.prev = prev;
  }

  public void setNext(CacheNode next) {
    this.next = next;
  }
}

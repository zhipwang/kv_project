package io.wzp.index.io;

/**
 * @author wzp
 * @since 2019/06/16
 */

public class CacheNodeList {

  private CacheNode dummy;
  private int nodeNumber;

  public CacheNodeList() {
    dummy = new CacheNode(-1, null);
    dummy.setNext(dummy);
    dummy.setPrev(dummy);
    this.nodeNumber = 0;
  }

  /**
   *
   * @return first node, null if empty
   */
  public CacheNode getFirst() {
    if (nodeNumber == 0) {
      return null;
    }

    return dummy.getNext();
  }

  /**
   *
   * @return last node, null if empty
   */
  public CacheNode getLast() {
    if (nodeNumber == 0) {
      return null;
    }

    return dummy.getPrev();
  }

  public void addFirst(CacheNode node) {
    node.setNext(dummy.getNext());
    node.setPrev(dummy);

    dummy.getNext().setPrev(node);
    dummy.setNext(node);

    nodeNumber++;
  }

  /**
   *
   * @return node, null if empty
   */
  public CacheNode removeFirst() {
    if (nodeNumber == 0) {
      return null;
    }

    nodeNumber--;

    CacheNode result = dummy.getNext();

    dummy.setNext(result.getNext());
    result.getNext().setPrev(dummy);

    result.setNext(null);
    result.setPrev(null);

    return result;
  }

  public CacheNode removeLast() {
    if (nodeNumber == 0) {
      return null;
    }

    nodeNumber--;

    CacheNode result = dummy.getPrev();

    dummy.setPrev(result.getPrev());
    result.getPrev().setNext(dummy);

    result.setNext(null);
    result.setPrev(null);

    return result;
  }

  public void remove(CacheNode node) {
    nodeNumber--;

    CacheNode prev = node.getPrev();
    prev.setNext(node.getNext());

    CacheNode next = node.getNext();
    next.setPrev(node.getPrev());

    node.setNext(null);
    node.setPrev(null);
  }
}

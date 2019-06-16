package io.wzp.index.io;

/**
 * @author wzp
 * @since 2019/06/15
 */

public class SkipNode {
  private final KVRecord record;
  private SkipNode[] skipTable;

  public SkipNode(KVRecord record, int height) {
    this.record = record;
    this.skipTable = new SkipNode[height];
  }

  public KVRecord getRecord() {
    return record;
  }

  public int comparesKey(byte[] key1) {
    return record.comparesToKey(key1);
  }

  public SkipNode getNext(int n) {
    assert n < skipTable.length : String.format("%d is out of capacity (%d)", n, skipTable.length);
    return skipTable[n];
  }

  public void setNext(int n, SkipNode node) {
    if (n >= skipTable.length) {
      expandNextPointerArray(n + 1);
    }

    skipTable[n] = node;
  }

  private void expandNextPointerArray(int height) {
    SkipNode[] newArr = new SkipNode[height];

    System.arraycopy(skipTable, 0, newArr, 0, skipTable.length);
    skipTable = newArr;
  }
}

package io.wzp.index.io;

import io.wzp.index.core.Conf;

import java.util.Iterator;
import java.util.Random;

/**
 * @author wzp
 * @since 2019/06/15
 */

public class SkipListImpl implements SkipList {

  private final static int MAX_HEIGHT = 12;
  private final static int BRANCHING = 4;

  private final Random rand;
  private final SkipNode head;
  private final long sizeLimit;

  private int curMaxHeight;
  private int curSize;

  public SkipListImpl(Conf conf) {
    sizeLimit = 1024L * 1024L * Integer.parseInt(conf.get(Conf.ConfVar.SKIP_LIST_SIZE_LIMIT_MB));

    head = new SkipNode(null, MAX_HEIGHT);
    rand = new Random();
    curMaxHeight = 1;

    for (int i = 0; i < MAX_HEIGHT; i++) {
      head.setNext(i, null);
    }

    curSize = 0;
  }

  /**
   *
   * @return true if full
   */
  public boolean isFull() {
    return curSize >= sizeLimit;
  }

  /**
   *
   * @return true if empty
   */
  public boolean isEmpty() {
    return head.getNext(0) == null;
  }

  /**
   * Reset skip list
   */
  public void reset() {
    curSize = 0;

    for (int i = 0; i < MAX_HEIGHT; i++) {
      head.setNext(i, null);
    }
  }

  /**
   * Put a record
   *
   * @param record record
   */
  public void put(KVRecord record) {
    SkipNode[] prev = new SkipNode[MAX_HEIGHT];

    findGreaterOrEqual(head, curMaxHeight - 1, record.getKey(), prev);

    int randomHeight = randomHeight();

    if (randomHeight > curMaxHeight) {
      for (int i = curMaxHeight; i < randomHeight; i++) {
        prev[i] = head;
      }

      curMaxHeight = randomHeight;
    }

    SkipNode newNode = new SkipNode(record, randomHeight);

    for (int i = 0; i < randomHeight; i++) {
      newNode.setNext(i, prev[i].getNext(i));
      prev[i].setNext(i, newNode);
    }

    curSize += record.getMemorySize();
  }

  /**
   *
   * @return first record, null if empty
   */
  public KVRecord getFirstRecord() {
    SkipNode firstNode = head.getNext(0);

    if (firstNode == null) {
      return null;
    }

    return firstNode.getRecord();
  }

  /**
   *
   * @return last record, null if empty
   */
  public KVRecord getLastRecord() {
    SkipNode lastNode = findLast(head, curMaxHeight - 1);

    if (lastNode == null) {
      return null;
    }

    return lastNode.getRecord();
  }

  /**
   * Get record by key
   *
   * @param key key
   * @return record
   */
  public KVRecord get(byte[] key) {
    if (isEmpty()) {
      return null;
    }

    boolean notInRange = getFirstRecord().comparesToKey(key) > 0 || getLastRecord().comparesToKey(key) < 0;

    if (notInRange) {
      return null;
    }

    SkipNode candidate = findGreaterOrEqual(head, curMaxHeight - 1, key, null);

    while (candidate != null && candidate.comparesKey(key) != 0) {
      candidate = candidate.getNext(0);
    }

    return candidate == null ? null : candidate.getRecord();
  }

  /**
   *
   * @return iterator
   */
  public Iterator<KVRecord> toIterator() {
    return new SkipListIterator();
  }

  private static boolean isRowKeyAfterNode(byte[] key, SkipNode node) {
    return node != null && node.getRecord() != null && node.comparesKey(key) < 0;
  }

  private static SkipNode findGreaterOrEqual(SkipNode tmp, int level, byte[] key, SkipNode[] prev) {
    while (true) {
      SkipNode next = tmp.getNext(level);

      if (isRowKeyAfterNode(key, next)) {
        tmp = next;
      } else {
        if (prev != null) {
          prev[level] = tmp;
        }

        if (level == 0) {
          return next;
        } else {
          level--;
        }
      }
    }
  }

  private static SkipNode findLast(SkipNode tmp, int level) {
    while (true) {
      SkipNode next = tmp.getNext(level);

      if (next == null) {
        if (level == 0) {
          return tmp;
        } else {
          level--;
        }
      } else {
        tmp = next;
      }
    }
  }

  private int randomHeight() {
    int height = 1;

    while (height < MAX_HEIGHT && (rand.nextInt() % BRANCHING) == 0) {
      height++;
    }

    return height;
  }


  private class SkipListIterator implements Iterator<KVRecord> {

    private SkipNode startNode = head.getNext(0);

    @Override
    public boolean hasNext() {
      return !(startNode == null || startNode.getRecord() == null);
    }

    @Override
    public KVRecord next() {
      KVRecord result = startNode.getRecord();
      startNode = startNode.getNext(0);

      return result;
    }
  }
}

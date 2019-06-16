package io.wzp.index.io;

import io.wzp.index.core.Conf;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.Iterator;
import java.util.Random;

/**
 * @author wzp
 * @since 2019/06/15
 */
public class SkipListTest {

  private KVRecord[] getRecordListInOrder(int number) {
    KVRecord[] result = new KVRecord[number];

    for (int i = 0; i < number; i++) {
      result[i] = new KVRecord(("" + i).getBytes(), "".getBytes(), true, -1, -1, -1);
    }

    return result;
  }

  private KVRecord[] getRecordListInDisOrder(int number) {
    KVRecord[] result = new KVRecord[number];
    Random random = new Random();

    for (int i = 0; i < number; i++) {
      result[i] = new KVRecord(("" + i).getBytes(), "".getBytes(), true, -1, -1, -1);
    }

    for (int i = 0; i < number; i++) {
      int randIndex = random.nextInt(number);

      KVRecord record = result[i];
      result[i] = result[randIndex];
      result[randIndex] = record;
    }

    return result;
  }

  @Test
  public void testGetPutInOrder() {
    SkipListImpl skipList = new SkipListImpl(new Conf());

    KVRecord[] records = getRecordListInOrder(10);

    for (KVRecord record : records) {
      skipList.put(record);
    }

    Assert.assertFalse(skipList.isEmpty());

    Assert.assertArrayEquals("0".getBytes(), skipList.getFirstRecord().getKey());
    Assert.assertArrayEquals("9".getBytes(), skipList.getLastRecord().getKey());
  }

  @Test
  public void testGetPutInDisOrder() {
    SkipListImpl skipList = new SkipListImpl(new Conf());

    KVRecord[] records = getRecordListInDisOrder(10);

    for (KVRecord record : records) {
      skipList.put(record);
    }

    Assert.assertFalse(skipList.isEmpty());

    Assert.assertArrayEquals("0".getBytes(), skipList.getFirstRecord().getKey());
    Assert.assertArrayEquals("9".getBytes(), skipList.getLastRecord().getKey());
  }

  @Test
  public void testIterator() throws IOException {
    SkipListImpl skipList = new SkipListImpl(new Conf());

    KVRecord[] records = getRecordListInDisOrder(10);

    for (KVRecord record : records) {
      skipList.put(record);
    }

    Iterator<KVRecord> iterator = skipList.toIterator();

    for (int i = 0; i < 10; i++) {
      Assert.assertArrayEquals(("" + i).getBytes(), iterator.next().getKey());
    }

    Assert.assertFalse(iterator.hasNext());
  }
}
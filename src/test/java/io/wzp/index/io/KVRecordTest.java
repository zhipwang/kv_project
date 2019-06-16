package io.wzp.index.io;

import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

import static org.junit.Assert.*;

/**
 * @author wzp
 * @since 2019/06/16
 */
public class KVRecordTest {

  @Test
  public void test_Ser_De_1() throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();

    byte[] key = new byte[] {1, 2, 3, 4};
    byte[] value = new byte[] {2, 3, 4, 5};
    new KVRecord(key, value, true, -1, -1, -1).writeTo(baos);

    KVRecord r1 = KVRecord.readFrom(ByteBuffer.wrap(baos.toByteArray()));

    Assert.assertArrayEquals(key, r1.getKey());
    Assert.assertArrayEquals(value, r1.getValue());
    Assert.assertTrue(r1.isValueWithKey());
    Assert.assertEquals(-1, r1.getValueFileNum());
    Assert.assertEquals(-1, r1.getValueFileOffset());
    Assert.assertEquals(-1, r1.getValueLength());
  }

  @Test
  public void test_Ser_De_2() throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();

    byte[] key = new byte[] {1, 2, 3, 4};
    new KVRecord(key, null, false, 1, 2, 3).writeTo(baos);

    KVRecord r1 = KVRecord.readFrom(ByteBuffer.wrap(baos.toByteArray()));

    Assert.assertArrayEquals(key, r1.getKey());
    Assert.assertNull(r1.getValue());
    Assert.assertFalse(r1.isValueWithKey());
    Assert.assertEquals(1, r1.getValueFileNum());
    Assert.assertEquals(2, r1.getValueFileOffset());
    Assert.assertEquals(3, r1.getValueLength());
  }
}
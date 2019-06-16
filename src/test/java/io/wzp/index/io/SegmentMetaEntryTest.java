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
public class SegmentMetaEntryTest {

  @Test
  public void test_Ser_De() throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();

    byte[] startKey = new byte[] {1, 2, 3, 4};
    byte[] endKey = new byte[] {2, 3, 4, 5};

    new SegmentMetaEntry(0, 1, startKey, endKey, 2, 3).writeTo(baos);
    baos.flush();

    SegmentMetaEntry entry = SegmentMetaEntry.readFrom(ByteBuffer.wrap(baos.toByteArray()));

    Assert.assertEquals(0, entry.getBlockId());
    Assert.assertArrayEquals(startKey, entry.getStartKey());
    Assert.assertArrayEquals(endKey, entry.getEndKey());
    Assert.assertEquals(1, entry.getRecordCount());
    Assert.assertEquals(2, entry.getBlockStartOffset());
    Assert.assertEquals(3, entry.getBlockSize());
  }
}
package io.wzp.index.io;

import junit.framework.Assert;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;

import static org.junit.Assert.*;

/**
 * @author wzp
 * @since 2019/06/16
 */
public class SegmentMetaBlockTest {

  @Test
  public void test_Ser_De() throws IOException {
    SegmentMetaBlock block = new SegmentMetaBlock();

    block.add(new SegmentMetaEntry(0, 0, new byte[] {1}, new byte[] {1, 9}, 0, 0));
    block.add(new SegmentMetaEntry(1, 0, new byte[] {2}, new byte[] {2, 9}, 0, 0));
    block.add(new SegmentMetaEntry(2, 0, new byte[] {3}, new byte[] {3, 9}, 0, 0));
    block.add(new SegmentMetaEntry(3, 0, new byte[] {4}, new byte[] {4, 9}, 0, 0));

    ByteArrayOutputStream baos = new ByteArrayOutputStream();

    block.writeTo(baos);
    baos.flush();

    SegmentMetaBlock block1 = SegmentMetaBlock.readFrom(ByteBuffer.wrap(baos.toByteArray()));

    int count = 0;
    Iterator<SegmentMetaEntry> iterator = block1.iterator();

    while (iterator.hasNext()) {
      iterator.next();
      count++;
    }

    Assert.assertEquals(4, count);
  }

  @Test
  public void test_Get() {
    SegmentMetaBlock block = new SegmentMetaBlock();

    block.add(new SegmentMetaEntry(0, 0, new byte[] {1}, new byte[] {1, 9}, 0, 0));
    block.add(new SegmentMetaEntry(1, 0, new byte[] {2}, new byte[] {2, 9}, 0, 0));
    block.add(new SegmentMetaEntry(2, 0, new byte[] {3}, new byte[] {3, 9}, 0, 0));
    block.add(new SegmentMetaEntry(3, 0, new byte[]{4}, new byte[]{4, 9}, 0, 0));

    Assert.assertNull(block.getEntry(new byte[]{0}));
    Assert.assertNull(block.getEntry(new byte[]{5}));
    Assert.assertEquals(0, block.getEntry(new byte[]{1}).getBlockId());
    Assert.assertEquals(0, block.getEntry(new byte[]{1, 0}).getBlockId());
    Assert.assertEquals(0, block.getEntry(new byte[]{1, 9}).getBlockId());
    Assert.assertEquals(1, block.getEntry(new byte[]{2, 0}).getBlockId());
  }
}
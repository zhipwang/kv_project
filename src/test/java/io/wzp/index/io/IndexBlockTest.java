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
public class IndexBlockTest {

  @Test
  public void test_Ser_De() throws IOException {
    IndexBlock block = new IndexBlock();

    block.addIndex(new byte[] {1}, 0);
    block.addIndex(new byte[] {1, 0}, 1);
    block.addIndex(new byte[]{1, 0, 0}, 2);
    block.addIndex(new byte[]{1, 0, 0, 0}, 3);
    block.addIndex(new byte[]{1, 0, 0, 0, 0}, 4);

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    block.writeTo(baos);
    baos.flush();

    IndexBlock block1 = IndexBlock.readFrom(ByteBuffer.wrap(baos.toByteArray()));

    Iterator<BlockIndexEntry> iterator = block1.iterator();

    int count = 0;

    while (iterator.hasNext()) {
      iterator.next();
      count++;
    }

    Assert.assertEquals(5, count);
  }

  @Test
  public void test_Get() {
    IndexBlock block = new IndexBlock();

    block.addIndex(new byte[] {1}, 0);
    block.addIndex(new byte[] {1, 1}, 1);
    block.addIndex(new byte[] {1, 1, 1}, 2);
    block.addIndex(new byte[] {1, 1, 1, 1}, 3);
    block.addIndex(new byte[]{1, 1, 1, 1, 1}, 4);


    Assert.assertNull(block.get(new byte[]{0}));
    Assert.assertNull(block.get(new byte[]{0, 2}));

    Assert.assertEquals(0, block.get(new byte[]{1}).getOffset());
    Assert.assertEquals(0, block.get(new byte[] {1, 0}).getOffset());
    Assert.assertEquals(0, block.get(new byte[] {1, 0, 1}).getOffset());
    Assert.assertEquals(1, block.get(new byte[] {1, 1}).getOffset());
    Assert.assertEquals(1, block.get(new byte[] {1, 1, 0}).getOffset());
    Assert.assertEquals(4, block.get(new byte[] {1, 1, 1, 1, 1}).getOffset());
    Assert.assertEquals(4, block.get(new byte[] {2}).getOffset());
  }
}
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
public class BlockIndexEntryTest {

  @Test
  public void test_Ser_De() throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();

    byte[] key = new byte[] {1, 2, 3, 4};

    new BlockIndexEntry(key, 0).writeTo(baos);
    baos.flush();

    BlockIndexEntry entry1 = BlockIndexEntry.readFrom(ByteBuffer.wrap(baos.toByteArray()));

    Assert.assertArrayEquals(key, entry1.getKey());
    Assert.assertEquals(0, entry1.getOffset());
  }
}
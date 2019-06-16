package io.wzp.index.io;

import junit.framework.Assert;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

import static org.junit.Assert.*;

/**
 * @author wzp
 * @since 2019/06/16
 */
public class BlockMetaEntryTest {

  @Test
  public void test_Ser_De() throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();

    new BlockMetaEntry(1, 2).writeTo(baos);
    baos.flush();

    BlockMetaEntry entry = BlockMetaEntry.readFrom(ByteBuffer.wrap(baos.toByteArray()));

    Assert.assertEquals(1, entry.getIndexOffset());
    Assert.assertEquals(2, entry.getIndexSize());
  }
}
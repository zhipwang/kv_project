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
public class TailBlockTest {

  @Test
  public void test_Ser_De() throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();

    new TailBlock(1, 2).writeTo(baos);
    baos.flush();

    TailBlock block = TailBlock.readFrom(ByteBuffer.wrap(baos.toByteArray()));

    Assert.assertEquals(1, block.getMetaOffset());
    Assert.assertEquals(2, block.getMetaSize());
  }
}
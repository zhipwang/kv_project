package io.wzp.index.utils;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

/**
 * @author wzp
 * @since 2019/06/16
 */

public class ByteBufferInputStream extends InputStream {

  private final ByteBuffer buffer;
  private final int sizeLimit;
  private int fetchedSize;

  public ByteBufferInputStream(ByteBuffer buffer, int offset, int sizeLimit) {
    this.buffer = buffer;
    buffer.position(offset);
    this.sizeLimit = sizeLimit;
  }

  @Override
  public int read() throws IOException {
    if (fetchedSize >= sizeLimit) {
      return -1;
    }

    fetchedSize++;

    return buffer.get();
  }

  @Override
  public int read(byte[] dest, int offset, int len) throws IOException {
    if (fetchedSize >= sizeLimit) {
      return -1;
    }

    int availableSize = Math.min(sizeLimit - fetchedSize, len);

    buffer.get(dest, offset, availableSize);

    fetchedSize += availableSize;

    return availableSize;
  }
}

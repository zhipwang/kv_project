package io.wzp.index.utils;

import sun.nio.ch.DirectBuffer;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

/**
 * @author wzp
 * @since 2019/06/15
 */

public class BufferHelper {

  public static void disposeMappedByteBuffer(ByteBuffer buffer) {
    if (buffer != null && buffer instanceof MappedByteBuffer) {
      if (((DirectBuffer) buffer).cleaner() != null) {
        ((DirectBuffer) buffer).cleaner().clean();
      }
    }
  }

  public static ByteBuffer getBuffer(FileChannel channel,
                                     boolean readOnly,
                                     long offset,
                                     int size) throws IOException {
    ByteBuffer buffer;

    if (channel == null || offset < 0 || size <= 0 || size >= Integer.MAX_VALUE) {
      return null;
    }

    if (readOnly) {
      buffer = channel.map(FileChannel.MapMode.READ_ONLY, offset, size);
    } else {
      buffer = channel.map(FileChannel.MapMode.READ_WRITE, offset, size);
    }

    return buffer;
  }
}

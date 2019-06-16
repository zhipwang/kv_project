package io.wzp.index.io;

import io.wzp.index.utils.BytesHelper;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;

/**
 * @author wzp
 * @since 2019/06/15
 */

public class TailBlock {
  public static final int TAIL_BLOCK_SIZE = 12;

  private final long metaOffset;
  private final int metaSize;

  public TailBlock(long metaOffset, int metaSize) {
    this.metaOffset = metaOffset;
    this.metaSize = metaSize;
  }

  public long getMetaOffset() {
    return metaOffset;
  }

  public int getMetaSize() {
    return metaSize;
  }

  public int writeTo(OutputStream out) throws IOException {
    out.write(BytesHelper.encodeLong(metaOffset));
    out.write(BytesHelper.encodeInt(metaSize));

    return TAIL_BLOCK_SIZE;
  }

  public static TailBlock readFrom(ByteBuffer buffer) {
    long f1 = buffer.getLong();
    int f2 = buffer.getInt();

    return new TailBlock(f1, f2);
  }
}

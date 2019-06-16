package io.wzp.index.io;

import io.wzp.index.utils.BytesHelper;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;

/**
 * @author wzp
 * @since 2019/06/15
 */

public class BlockMetaEntry {

  public static final int BLOCK_META_ENTRY_SIZE = 8;

  private final int indexOffset;
  private final int indexSize;

  public BlockMetaEntry(int indexOffset, int indexSize) {
    this.indexOffset = indexOffset;
    this.indexSize = indexSize;
  }

  public int getIndexOffset() {
    return indexOffset;
  }

  public int getIndexSize() {
    return indexSize;
  }

  public int writeTo(OutputStream out) throws IOException {
    out.write(BytesHelper.encodeInt(indexOffset));
    out.write(BytesHelper.encodeInt(indexSize));

    return BLOCK_META_ENTRY_SIZE;
  }

  public static BlockMetaEntry readFrom(ByteBuffer buffer) {
    int l1 = buffer.getInt();
    int l2 = buffer.getInt();

    return new BlockMetaEntry(l1, l2);
  }
}

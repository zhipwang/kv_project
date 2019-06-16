package io.wzp.index.io;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * @author wzp
 * @since 2019/06/15
 */

public class DataBlock {

  private final ByteBuffer buffer;
  private BlockMetaEntry metaEntry;
  private IndexBlock indexBlock;

  public DataBlock(ByteBuffer buffer) {
    this.buffer = buffer;
  }

  public void init() throws IOException {
    buffer.position(buffer.capacity() - BlockMetaEntry.BLOCK_META_ENTRY_SIZE);
    metaEntry = BlockMetaEntry.readFrom(buffer);

    buildIndexBlock();
  }

  public ByteBuffer getBuffer() {
    return buffer;
  }

  public int getSize() {
    return buffer.capacity();
  }

  private void buildIndexBlock() throws IOException {
    buffer.position(metaEntry.getIndexOffset());
    indexBlock = IndexBlock.readFrom(buffer);
  }

  /**
   *
   * @param targetKey key
   * @return record, null if not found
   * @throws IOException
   */
  public KVRecord get(byte[] targetKey) throws IOException {
    BlockIndexEntry indexEntry = indexBlock.get(targetKey);

    if (indexEntry == null) {
      return null;
    }

    buffer.position(indexEntry.getOffset());

    int maxOffset = metaEntry.getIndexOffset();

    while (buffer.position() < maxOffset) {
      KVRecord r1 = KVRecord.readFrom(buffer);
      int cmp = r1.comparesToKey(targetKey);

      if (cmp == 0) {
        return r1;
      } else if (cmp > 1) {
        break;
      }
    }

    return null;
  }
}

package io.wzp.index.io;

import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnels;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;

/**
 * @author wzp
 * @since 2019/06/15
 */

public class DataBlockWriter {

  private final OutputStream fileOutputStream;
  private final int blockSizeLimit;
  private final int indexSpan;

  private int recordCount = 0;
  private int blockSize = 0;
  private long startOffset = 0;

  private byte[] startKey = null;
  private byte[] endKey = null;
  private int blockId = 0;

  private IndexBlock indexBlock;

  public DataBlockWriter(OutputStream fileOutputStream,
                         int blockSizeLimit,
                         int indexSpan) {
    this.fileOutputStream = fileOutputStream;
    this.blockSizeLimit = blockSizeLimit;
    this.indexBlock = new IndexBlock();
    this.indexSpan = indexSpan;
  }

  public int getBlockSize() {
    return blockSize;
  }

  public long getStartOffset() {
    return startOffset;
  }

  /**
   * Put record
   *
   * @param record record
   * @throws IOException
   */
  public void put(KVRecord record) throws IOException {
    //add index entry
    if (recordCount % indexSpan == 0) {
      indexBlock.addIndex(record.getKey(), blockSize);
    }

    blockSize += record.writeTo(fileOutputStream);

    if (startKey == null) {
      startKey = record.getKey();
    }

    endKey = record.getKey();

    recordCount++;
  }

  private void reset() {
    //segment file offset
    startOffset += blockSize;

    recordCount = 0;
    blockSize = 0;
    startKey = null;
    endKey = null;

    indexBlock.reset();
  }

  public boolean isFull() {
    return blockSize >= blockSizeLimit;
  }

  public boolean isEmpty() {
    return blockSize == 0;
  }

  /**
   * Flush data block
   *
   * @return entry, null if no data
   * @throws IOException
   */
  public SegmentMetaEntry flush() throws IOException {
    if (!isEmpty()) {
      //block index start offset
      int blockIndexOffset = blockSize;

      int blockIndexSize = indexBlock.writeTo(fileOutputStream);

      blockSize += blockIndexSize;

      //block meta entry
      BlockMetaEntry me = new BlockMetaEntry(blockIndexOffset, blockIndexSize);

      blockSize += me.writeTo(fileOutputStream);

      //segment meta entry
      SegmentMetaEntry sme = new SegmentMetaEntry(
        blockId++,
        recordCount,
        startKey,
        endKey,
        startOffset,
        blockSize
      );

      //reset
      reset();

      return sme;
    }

    return null;
  }
}

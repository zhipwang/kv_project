package io.wzp.index.io;

import io.wzp.index.utils.BytesHelper;
import io.wzp.index.utils.VarNumberHelper;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;

/**
 * @author wzp
 * @since 2019/06/15
 */

public class SegmentMetaEntry {
  private final int blockId;
  private final int recordCount;
  private final byte[] startKey;
  private final byte[] endKey;
  private final long blockStartOffset;
  private final int blockSize;

  public SegmentMetaEntry(int blockId, int recordCount,
                          byte[] startKey, byte[] endKey,
                          long blockStartOffset, int blockSize) {
    this.blockId = blockId;
    this.recordCount = recordCount;
    this.startKey = startKey;
    this.endKey = endKey;
    this.blockStartOffset = blockStartOffset;
    this.blockSize = blockSize;
  }

  public int getBlockId() {
    return blockId;
  }

  public int getRecordCount() {
    return recordCount;
  }

  public byte[] getStartKey() {
    return startKey;
  }

  public byte[] getEndKey() {
    return endKey;
  }

  public long getBlockStartOffset() {
    return blockStartOffset;
  }

  public int getBlockSize() {
    return blockSize;
  }

  public int comparesTo(byte[] targetKey) {
    int leftCmp = BytesHelper.compareByteArray(startKey, targetKey);

    if (leftCmp > 0) {
      return 1;
    }

    int rightCmp = BytesHelper.compareByteArray(endKey, targetKey);

    if (rightCmp < 0) {
      return -1;
    }

    return 0;
  }

  public int writeTo(OutputStream out) throws IOException {
    int totalSize = 0;

    //block id
    byte[] varBlockId = VarNumberHelper.encodeUnsignedVarInt(blockId);
    out.write(varBlockId);

    totalSize += varBlockId.length;

    //record count
    byte[] varRecordCount = VarNumberHelper.encodeUnsignedVarInt(recordCount);
    out.write(recordCount);

    totalSize += varRecordCount.length;

    //start key
    byte[] varStartKeyLen = VarNumberHelper.encodeUnsignedVarInt(startKey.length);
    out.write(varStartKeyLen);
    out.write(startKey);

    totalSize += varStartKeyLen.length + startKey.length;

    //end key
    byte[] varEndKeyLen = VarNumberHelper.encodeUnsignedVarInt(endKey.length);
    out.write(varEndKeyLen);
    out.write(endKey);

    totalSize += varEndKeyLen.length + endKey.length;

    //block start offset
    byte[] varBlockOffset = VarNumberHelper.encodeUnsignedVarLong(blockStartOffset);
    out.write(varBlockOffset);

    totalSize += varBlockOffset.length;

    //block size
    byte[] varBlockSize = VarNumberHelper.encodeUnsignedVarInt(blockSize);
    out.write(varBlockSize);

    totalSize += varBlockSize.length;

    return totalSize;
  }

  public static SegmentMetaEntry readFrom(ByteBuffer buffer) throws IOException {
    int f1 = VarNumberHelper.decodeUnsignedVarInt(buffer);
    //record count
    int f2 = VarNumberHelper.decodeUnsignedVarInt(buffer);

    //start key
    int l1 = VarNumberHelper.decodeUnsignedVarInt(buffer);

    if (l1 < 0) {
      throw new IOException(String.format("Negative start key length (%d)", l1));
    }

    byte[] key1 = new byte[l1];
    buffer.get(key1);

    //end key
    int l2 = VarNumberHelper.decodeUnsignedVarInt(buffer);

    if (l2 < 0) {
      throw new IOException(String.format("Negative start key length (%d)", l2));
    }

    byte[] key2 = new byte[l2];
    buffer.get(key2);

    //block offset
    long f3 = VarNumberHelper.decodeUnsignedVarLong(buffer);

    //block size
    int f4 = VarNumberHelper.decodeUnsignedVarInt(buffer);

    return new SegmentMetaEntry(f1, f2, key1, key2, f3, f4);
  }
}

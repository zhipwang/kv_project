package io.wzp.index.io;

import io.wzp.index.utils.BytesHelper;
import io.wzp.index.utils.VarNumberHelper;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * @author wzp
 * @since 2019/06/15
 */

public class IndexBlock {

  private final List<BlockIndexEntry> list;

  public IndexBlock() {
    this.list = new ArrayList<>();
  }

  public IndexBlock(int n) {
    assert n > 0 : String.format("Negative capacity (%d)", n);
    this.list = new ArrayList<>(n);
  }

  public void addIndex(byte[] key, int offset) {
    list.add(new BlockIndexEntry(key, offset));
  }

  public void addIndex(BlockIndexEntry entry) {
    list.add(entry);
  }

  public void reset() {
    list.clear();
  }

  /**
   * Get block index entry with target key
   *
   * @param targetKey target key
   * @return entry, null if index is empty or the target key is not in range
   */
  public BlockIndexEntry get(byte[] targetKey) {
    if (list.isEmpty()) {
      return null;
    }

    int left = 0, right = list.size() - 1;
    int middle = (left + right) / 2;

    while (left <= right) {
      int cmp1 = BytesHelper.compareByteArray(list.get(middle).getKey(), targetKey);

      if (cmp1 <= 0) {
        if (middle + 1 < list.size()) {
          int cmp2 = BytesHelper.compareByteArray(list.get(middle + 1).getKey(), targetKey);

          if (cmp2 == 0) {
            return list.get(middle + 1);
          } else if (cmp2 > 0) {
            return list.get(middle);
          } else {
            left = middle + 1;
          }
        } else {
          return list.get(middle);
        }
      } else {
        right = middle - 1;
      }

      middle = (left + right) / 2;
    }

    return null;
  }

  public Iterator<BlockIndexEntry> iterator() {
    return list.iterator();
  }

  public int writeTo(OutputStream out) throws IOException {
    int totalSize = 0;

    //element number
    byte[] varCount = VarNumberHelper.encodeUnsignedVarInt(list.size());
    out.write(varCount);
    totalSize += varCount.length;

    for (BlockIndexEntry entry : list) {
      totalSize += entry.writeTo(out);
    }

    return totalSize;
  }

  public static IndexBlock readFrom(ByteBuffer buffer) throws IOException {
    int count = VarNumberHelper.decodeUnsignedVarInt(buffer);

    IndexBlock indexBlock = new IndexBlock(count);

    for (int i = 0; i < count; i++) {
      indexBlock.addIndex(BlockIndexEntry.readFrom(buffer));
    }

    return indexBlock;
  }
}

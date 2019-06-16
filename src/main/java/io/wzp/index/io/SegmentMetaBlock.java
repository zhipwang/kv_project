package io.wzp.index.io;

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

public class SegmentMetaBlock {

  private final List<SegmentMetaEntry> list;

  public SegmentMetaBlock() {
    this.list = new ArrayList<>();
  }

  public SegmentMetaBlock(int n) {
    this.list = new ArrayList<>(n);
  }

  /**
   *
   * @param entry entry
   */
  public void add(SegmentMetaEntry entry) {
    list.add(entry);
  }

  /**
   *
   * @return first key of the block, null if empty
   */
  public byte[] getStartKey() {
    if (list.isEmpty()) {
      return null;
    }

    return list.get(0).getStartKey();
  }

  /**
   *
   * @return last key of the block, null if empty
   */
  public byte[] getEndKey() {
    if (list.isEmpty()) {
      return null;
    }

    return list.get(list.size() - 1).getEndKey();
  }

  /**
   *
   * @param targetKey key
   * @return meta entry, null if no key match
   */
  public SegmentMetaEntry getEntry(byte[] targetKey) {
    int left = 0, right = list.size() - 1;
    int middle = (left + right) / 2;

    while (left <= right) {
      SegmentMetaEntry entry = list.get(middle);

      int cmp = entry.comparesTo(targetKey);

      if (cmp == 0) {
        return entry;
      } else if (cmp < 0) {
        left = middle + 1;
      } else {
        right = middle - 1;
      }

      middle = (left + right) / 2;
    }

    return null;
  }

  public Iterator<SegmentMetaEntry> iterator() {
    return list.iterator();
  }

  public int writeTo(OutputStream out) throws IOException {
    int totalSize = 0;

    //num
    byte[] varNum = VarNumberHelper.encodeUnsignedVarInt(list.size());
    out.write(varNum);

    totalSize += varNum.length;

    //entries
    for (SegmentMetaEntry entry : list) {
      totalSize += entry.writeTo(out);
    }

    return totalSize;
  }

  public static SegmentMetaBlock readFrom(ByteBuffer buffer) throws IOException {
    int count = VarNumberHelper.decodeUnsignedVarInt(buffer);

    SegmentMetaBlock block = new SegmentMetaBlock(count);

    for (int i = 0; i < count; i++) {
      block.add(SegmentMetaEntry.readFrom(buffer));
    }

    return block;
  }
}

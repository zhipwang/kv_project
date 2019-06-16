package io.wzp.index.io;

import io.wzp.index.utils.VarNumberHelper;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;

/**
 * @author wzp
 * @since 2019/06/15
 */

public class BlockIndexEntry {

  private final byte[] key;
  private final int offset;

  public BlockIndexEntry(byte[] key, int offset) {
    this.key = key;
    this.offset = offset;
  }

  public byte[] getKey() {
    return key;
  }

  public int getOffset() {
    return offset;
  }

  public int writeTo(OutputStream out) throws IOException {
    //key len
    byte[] varKeyLen = VarNumberHelper.encodeUnsignedVarInt(key.length);
    out.write(varKeyLen);

    //key
    out.write(key);

    //offset
    byte[] varOffset = VarNumberHelper.encodeUnsignedVarInt(offset);
    out.write(varOffset);

    return varKeyLen.length + key.length + varOffset.length;
  }

  public static BlockIndexEntry readFrom(ByteBuffer buffer) throws IOException {
    //get key
    int keyLen = VarNumberHelper.decodeUnsignedVarInt(buffer);

    if (keyLen < 0) {
      throw new IOException(String.format("Negative key length (%d)", keyLen));
    }

    byte[] key = new byte[keyLen];

    buffer.get(key);

    //get offset
    int offset = VarNumberHelper.decodeUnsignedVarInt(buffer);

    return new BlockIndexEntry(key, offset);
  }
}

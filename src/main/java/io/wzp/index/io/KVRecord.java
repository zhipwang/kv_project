package io.wzp.index.io;

import io.wzp.index.utils.BytesHelper;
import io.wzp.index.utils.VarNumberHelper;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;

/**
 *
 * @author wzp
 * @since 2019/06/15
 */

public class KVRecord {
  private final byte[] key;
  private final byte[] value;
  private final boolean valueWithKey;
  private final int valueFileNum;
  private final long valueFileOffset;
  private final int valueLength;

  public KVRecord(byte[] key, byte[] value, boolean valueWithKey,
                  int valueFileNum, long valueFileOffset, int valueLength) {
    this.key = key;
    this.value = value;
    this.valueWithKey = valueWithKey;
    this.valueFileNum = valueFileNum;
    this.valueFileOffset = valueFileOffset;
    this.valueLength = valueLength;
  }

  public byte[] getKey() {
    return key;
  }

  public byte[] getValue() {
    return value;
  }

  public boolean isValueWithKey() {
    return valueWithKey;
  }

  public int getValueFileNum() {
    return valueFileNum;
  }

  public long getValueFileOffset() {
    return valueFileOffset;
  }

  public int getValueLength() {
    return valueLength;
  }

  public int getMemorySize() {
    return key.length + (value == null ? 0 : value.length) + 13;
  }

  public int comparesToKey(byte[] key1) {
    if (key1 == null) {
      return 1;
    }

    return BytesHelper.compareByteArray(key, key1);
  }

  public int writeTo(OutputStream out) throws IOException {
    //key
    byte[] varKeyLen = VarNumberHelper.encodeUnsignedVarInt(key.length);

    out.write(varKeyLen);
    out.write(key);

    //mark
    out.write(valueWithKey ? (byte) 1 : (byte) 0);

    if (valueWithKey) {
      //value
      byte[] varValueLen = VarNumberHelper.encodeUnsignedVarInt(value.length);

      out.write(varValueLen);
      out.write(value);

      return key.length + varKeyLen.length + value.length + varValueLen.length + 1;
    } else {
      //file seq
      byte[] varValueFileNum = VarNumberHelper.encodeUnsignedVarInt(valueFileNum);
      out.write(varValueFileNum);
      //file offset
      byte[] varValueFileOffset = VarNumberHelper.encodeUnsignedVarLong(valueFileOffset);
      out.write(varValueFileOffset);
      //value length
      byte[] varValueLength = VarNumberHelper.encodeUnsignedVarInt(valueLength);
      out.write(varValueLength);

      return key.length + varKeyLen.length + varValueFileNum.length +
        varValueFileOffset.length + varValueLength.length + 1;
    }
  }

  public static KVRecord readFrom(ByteBuffer buffer) throws IOException {
    //key
    int keyLen = VarNumberHelper.decodeUnsignedVarInt(buffer);

    if (keyLen < 0) {
      throw new IOException(String.format("Negative key length (%d)", keyLen));
    }

    byte[] key = new byte[keyLen];
    buffer.get(key);

    //mark
    boolean embeddedValue = buffer.get() == 1;

    //value
    byte[] value = null;
    int tmpValueFileNum = -1;
    long tmpValueFileOffset = -1;
    int valueLength = -1;

    if (embeddedValue) {
      int valueLen = VarNumberHelper.decodeUnsignedVarInt(buffer);

      if (valueLen < 0) {
        throw new IOException(String.format("Negative value length (%d)", valueLen));
      }

      value = new byte[valueLen];
      buffer.get(value);
    } else {
      //file seq
      tmpValueFileNum = VarNumberHelper.decodeUnsignedVarInt(buffer);
      //file offset
      tmpValueFileOffset = VarNumberHelper.decodeUnsignedVarLong(buffer);
      //value length
      valueLength = VarNumberHelper.decodeUnsignedVarInt(buffer);
    }

    return new KVRecord(key, value, embeddedValue, tmpValueFileNum, tmpValueFileOffset, valueLength);
  }
}

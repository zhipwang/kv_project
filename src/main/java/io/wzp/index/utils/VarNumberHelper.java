package io.wzp.index.utils;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Encode & decode unsigned varint
 *
 * @author wzp
 * @since 2019/06/15
 */

public class VarNumberHelper {

  private VarNumberHelper() {

  }

  /**
   * Encode an unsigned int value
   *
   * @param value value
   * @return encoded int
   */
  public static byte[] encodeUnsignedVarInt(int value) {
    byte[] byteArrayList = new byte[10];
    int i = 0;

    while ((value & 0xFFFFFF80) != 0L) {
      byteArrayList[i++] = (byte) ((value & 0x7F) | 0x80);
      value >>>= 7;
    }

    byteArrayList[i] = (byte) (value & 0x7F);

    byte[] out = new byte[i + 1];

    for (; i >= 0; i--) {
      out[i] = byteArrayList[i];
    }

    return out;
  }

  /**
   * Decode a varint to an unsigned normal int
   *
   * @param buffer buffer
   * @param offset offset
   * @return normal int
   * @throws IOException exception
   */
  public static int decodeUnsignedVarInt(byte[] buffer, int offset) throws IOException {
    int value = 0;
    int i = 0;
    int b;

    for (int pos = offset; pos < buffer.length; pos++) {
      b = buffer[pos];

      if ((b & 0x80) == 0) {
        return value | (b << i);
      }

      value |= (b & 0x7F) << i;
      i += 7;

      if (i > 35) {
        throw new IllegalArgumentException("Variable length quantity is too long");
      }
    }

    throw new IOException("Invalid buffer");
  }

  /**
   * Decode a varint to an unsigned int
   *
   * @param buffer buffer
   * @return int
   */
  public static int decodeUnsignedVarInt(ByteBuffer buffer) {
    int value = 0;
    int i = 0;
    int b;

    while (((b = buffer.get()) & 0x80) != 0) {
      value |= (b & 0x7F) << i;
      i += 7;
      if (i > 35) {
        throw new IllegalArgumentException("Variable length quantity is too long");
      }
    }

    return value | (b << i);
  }

  /**
   * Encode an unsigned var long
   *
   * @param value long
   * @return encoded long
   */
  public static byte[] encodeUnsignedVarLong(long value) {
    byte[] byteArrayList = new byte[10];
    int i = 0;

    while ((value & 0xFFFFFFFFFFFFFF80L) != 0L) {
      byteArrayList[i++] = (byte) (((int) value & 0x7F) | 0x80);
      value >>>= 7;
    }

    byteArrayList[i] = (byte) ((int) value & 0x7F);

    byte[] out = new byte[i + 1];

    for (; i >= 0; i--) {
      out[i] = byteArrayList[i];
    }

    return out;
  }

  /**
   * Decode unsigned varlong
   *
   * @param buffer buffer
   * @param offset offer
   * @return long value
   * @throws IOException
   */
  public static long decodeUnsignedVarLong(byte[] buffer, int offset) throws IOException {
    long value = 0L;
    int i = 0;
    long b;

    for (int pos = offset; pos < buffer.length; pos++) {
      b = buffer[pos];

      if ((b & 0x80L) == 0) {
        return value | (b << i);
      }

      value |= (b & 0x7F) << i;
      i += 7;

      if (i > 63) {
        throw new IllegalArgumentException("Variable length quantity is too long");
      }
    }

    throw new IOException("Invalid buffer");
  }

  /**
   * Decode unsigned varlong
   *
   * @param buffer buffer
   * @return long value
   */
  public static long decodeUnsignedVarLong(ByteBuffer buffer) {
    long value = 0L;
    int i = 0;
    long b;

    while (((b = buffer.get()) & 0x80L) != 0) {
      value |= (b & 0x7F) << i;
      i += 7;
      if (i > 63) {
        throw new IllegalArgumentException("Variable length quantity is too long");
      }
    }

    return value | (b << i);
  }
}

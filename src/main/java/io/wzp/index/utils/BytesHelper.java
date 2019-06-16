package io.wzp.index.utils;

import sun.misc.Unsafe;

import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.security.AccessController;
import java.security.PrivilegedAction;

/**
 * @author wzp
 * @since 2019/06/15
 */

public class BytesHelper {
  private BytesHelper() {

  }


  static final Unsafe THE_UNSAFE;
  static long BYTE_ARRAY_BASE_OFFSET;
  static long CHAR_ARRAY_BASE_OFFSET;
  static long BOOLEAN_ARRAY_BASE_OFFSET;
  static long SHORT_ARRAY_BASE_OFFSET;
  static long INT_ARRAY_BASE_OFFSET;
  static long LONG_ARRAY_BASE_OFFSET;
  static long FLOAT_ARRAY_BASE_OFFSET;
  static long DOUBLE_ARRAY_BASE_OFFSET;
  static final int SIZEOF_LONG = 8;
  static final boolean LITTLE_ENDIAN = ByteOrder.nativeOrder().equals(ByteOrder.LITTLE_ENDIAN);

  static {
    THE_UNSAFE = (Unsafe) AccessController.doPrivileged(
      (PrivilegedAction<Object>) () -> {
        try {
          Field f = Unsafe.class.getDeclaredField("theUnsafe");
          f.setAccessible(true);
          return f.get(null);
        } catch (NoSuchFieldException | IllegalAccessException e) {
          throw new Error();
        }
      }
    );

    BYTE_ARRAY_BASE_OFFSET = THE_UNSAFE.arrayBaseOffset(byte[].class);
    CHAR_ARRAY_BASE_OFFSET = THE_UNSAFE.arrayBaseOffset(char[].class);
    BOOLEAN_ARRAY_BASE_OFFSET = THE_UNSAFE.arrayBaseOffset(boolean[].class);
    SHORT_ARRAY_BASE_OFFSET = THE_UNSAFE.arrayBaseOffset(short[].class);
    INT_ARRAY_BASE_OFFSET = THE_UNSAFE.arrayBaseOffset(int[].class);
    LONG_ARRAY_BASE_OFFSET = THE_UNSAFE.arrayBaseOffset(long[].class);
    FLOAT_ARRAY_BASE_OFFSET = THE_UNSAFE.arrayBaseOffset(float[].class);
    DOUBLE_ARRAY_BASE_OFFSET = THE_UNSAFE.arrayBaseOffset(double[].class);

    if (THE_UNSAFE.arrayIndexScale(byte[].class) != 1) {
      throw new AssertionError();
    }
  }

  static boolean lessThanUnsigned(long x1, long x2) {
    return (x1 + Long.MIN_VALUE) < (x2 + Long.MIN_VALUE);
  }

  /**
   * Compare two byte arrays
   * @param buffer1 buffer1
   * @param buffer2 buffer2
   * @return 0 if equals; 1 if buffer1 is larger than buffer2; -1 if buffer2 is smaller than buffer1
   */
  public static int compareByteArray(byte[] buffer1, byte[] buffer2) {
    return compareByteArray(buffer1, 0, buffer1.length, buffer2, 0, buffer2.length);
  }


  public static int compareByteArray(byte[] buffer1, int offset1, int length1,
                                     byte[] buffer2, int offset2, int length2) {

    if (buffer1 == buffer2 && offset1 == offset2 && length1 == length2) {
      return 0;
    }

    int minLength = Math.min(length1, length2);
    int minWords = minLength / SIZEOF_LONG;
    long offset1Adj = offset1 + BYTE_ARRAY_BASE_OFFSET;
    long offset2Adj = offset2 + BYTE_ARRAY_BASE_OFFSET;

    for (int i = 0; i < minWords * SIZEOF_LONG; i += SIZEOF_LONG) {
      long lw = THE_UNSAFE.getLong(buffer1, offset1Adj + i);
      long rw = THE_UNSAFE.getLong(buffer2, offset2Adj + i);
      long diff = lw ^ rw;

      if (diff != 0) {
        if (!LITTLE_ENDIAN) {
          return lessThanUnsigned(lw, rw) ? -1 : 1;
        }

        // Use binary search
        int n = 0;
        int y;
        int x = (int) diff;
        if (x == 0) {
          x = (int) (diff >>> 32);
          n = 32;
        }

        y = x << 16;
        if (y == 0) {
          n += 16;
        } else {
          x = y;
        }

        y = x << 8;
        if (y == 0) {
          n += 8;
        }
        return (int) (((lw >>> n) & 0xFFL) - ((rw >>> n) & 0xFFL));
      }
    }

    // The epilogue to cover the last (minLength % 8) elements.
    for (int i = minWords * SIZEOF_LONG; i < minLength; i++) {
      int a = (buffer1[offset1 + i] & 0xFF);
      int b = (buffer2[offset2 + i] & 0xFF);
      if (a != b) {
        return a - b;
      }
    }

    return length1 - length2;
  }

  public static byte[] encodeInt(int value) {
    ByteBuffer buffer = ByteBuffer.allocate(4);

    buffer.putInt(value);

    return buffer.array();
  }

  public static byte[] encodeLong(long value) {
    ByteBuffer buffer = ByteBuffer.allocate(8);

    buffer.putLong(value);

    return buffer.array();
  }
}

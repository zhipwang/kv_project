package io.wzp.index.utils;

import junit.framework.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;

/**
 * @author wzp
 * @since 2019/06/15
 */
@RunWith(Parameterized.class)
public class VarNumberHelperTest {

  enum Type {UNSIGNED_INT, UNSIGNED_LONG}

  @Parameterized.Parameters
  public static Collection<Object[]> data() {
    return Arrays.asList(new Object[][]{
      {Type.UNSIGNED_INT, 0, 0},
      {Type.UNSIGNED_INT, 1, 1},
      {Type.UNSIGNED_INT, 127, 127},
      {Type.UNSIGNED_INT, Integer.MAX_VALUE, Integer.MAX_VALUE},
      {Type.UNSIGNED_LONG, 0, 0},
      {Type.UNSIGNED_LONG, 1, 1},
      {Type.UNSIGNED_LONG, Long.MAX_VALUE, Long.MAX_VALUE},
    });
  }

  private Type type;
  private long input;
  private long expect;

  public VarNumberHelperTest(Type type, long input, long expect) {
    this.type = type;
    this.input = input;
    this.expect = expect;
  }

  @Test
  public void test_Encode_Unsigned_Int() throws IOException {
    switch (type) {
      case UNSIGNED_INT:
        byte[] encodeData = VarNumberHelper.encodeUnsignedVarInt((int) input);
        Assert.assertEquals(expect, VarNumberHelper.decodeUnsignedVarInt(encodeData, 0));
        break;

      default: break;
    }
  }

  @Test
  public void test_Encode_Unsigned_Int_WithBuffer() throws IOException {
    switch (type) {
      case UNSIGNED_INT:
        byte[] encodeData = VarNumberHelper.encodeUnsignedVarInt((int) input);
        ByteBuffer newBuffer = ByteBuffer.wrap(encodeData);

        Assert.assertEquals(expect, VarNumberHelper.decodeUnsignedVarInt(newBuffer));
        break;

      default: break;
    }
  }

  @Test
  public void test_Encode_Unsigned_Long() throws IOException {
    switch (type) {
      case UNSIGNED_LONG:
        byte[] encodeData = VarNumberHelper.encodeUnsignedVarLong(input);
        Assert.assertEquals(expect, VarNumberHelper.decodeUnsignedVarLong(encodeData, 0));
        break;

      default: break;
    }
  }

  @Test
  public void test_Encode_Unsigned_Long_WithBuffer() throws IOException {
    switch (type) {
      case UNSIGNED_LONG:
        byte[] encodeData = VarNumberHelper.encodeUnsignedVarLong(input);
        ByteBuffer newBuffer = ByteBuffer.wrap(encodeData);

        Assert.assertEquals(expect, VarNumberHelper.decodeUnsignedVarLong(newBuffer));
        break;

      default: break;
    }
  }
}
package io.wzp.index.io;

import io.wzp.index.utils.BufferHelper;
import io.wzp.index.utils.BytesHelper;
import junit.framework.Assert;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static org.junit.Assert.*;

/**
 * @author wzp
 * @since 2019/06/16
 */
public class DataBlockWriterTest {

  @Rule
  public TemporaryFolder folder = new TemporaryFolder();

  private OutputStream out;
  private File file;

  @Before
  public void setUp() throws IOException {
    file = folder.newFile();
    out = new FileOutputStream(file);
  }

  @After
  public void tearDown() throws IOException {
    out.close();
  }

  private List<KVRecord> generateData() {
    Random random = new Random();
    int num = random.nextInt(100_000);

    List<KVRecord> result = new ArrayList<>(num);

    for (int i = 0; i < num; i += random.nextInt(10)) {
      result.add(new KVRecord(BytesHelper.encodeInt(i), new byte[] {0}, true, -1, -1, -1));
    }

    return result;
  }

  private List<KVRecord> generateData(int num) {
    List<KVRecord> result = new ArrayList<>(num);

    for (int i = 0; i < num; i++) {
      result.add(new KVRecord(BytesHelper.encodeInt(i), new byte[] {0}, true, -1, -1, -1));
    }

    return result;
  }

  @Test
  public void test_Write_SingleFlush() throws IOException {
    DataBlockWriter writer = new DataBlockWriter(out, 16 * 1024 * 1024, 1000);

    int num = 100;

    List<KVRecord> records = generateData(num);

    for (KVRecord record : records) {
      writer.put(record);
    }

    writer.flush();
    out.flush();
    out.close();


    //test read
    RandomAccessFile raf = new RandomAccessFile(file, "r");
    ByteBuffer buffer = null;

    try {
      buffer = BufferHelper.getBuffer(raf.getChannel(), true, 0, (int) raf.length());

      DataBlock dataBlock = new DataBlock(buffer);
      dataBlock.init();

      for (int i = 0; i < num; i++) {
        Assert.assertNotNull(dataBlock.get(BytesHelper.encodeInt(i)));
      }

      Assert.assertNull(dataBlock.get(new byte[] {-1}));
      Assert.assertNull(dataBlock.get(new byte[] {101}));
    } finally {
      if (buffer != null) {
        BufferHelper.disposeMappedByteBuffer(buffer);
      }
    }
  }

  @Test
  public void test_Write_MultiFlush() throws IOException {
    DataBlockWriter writer = new DataBlockWriter(out, 16 * 1024 * 1024, 1000);

    List<KVRecord> records = generateData();

    for (KVRecord record : records) {
      writer.put(record);
    }

    writer.flush();
    out.flush();
    out.close();

    //test read
    RandomAccessFile raf = new RandomAccessFile(file, "r");
    ByteBuffer buffer = null;

    try {
      buffer = BufferHelper.getBuffer(raf.getChannel(), true, 0, (int) raf.length());

      DataBlock dataBlock = new DataBlock(buffer);
      dataBlock.init();

      for (KVRecord record : records) {
        Assert.assertNotNull(dataBlock.get(record.getKey()));
      }

      Assert.assertNull(dataBlock.get(new byte[] {-1}));
    } finally {
      if (buffer != null) {
        BufferHelper.disposeMappedByteBuffer(buffer);
      }
    }
  }
}
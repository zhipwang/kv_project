package io.wzp.index.io;

import io.wzp.index.core.Conf;
import io.wzp.index.utils.BytesHelper;
import junit.framework.Assert;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * @author wzp
 * @since 2019/06/16
 */
public class SegmentTest {

  @Rule
  public TemporaryFolder folder = new TemporaryFolder();

  private File file;

  @Before
  public void setUp() throws IOException {
    file = folder.newFile();
  }

  @After
  public void tearDown() throws IOException {

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
  public void test_Write_Single_DataBlock() throws IOException {
    Conf conf = new Conf();
    SegmentImpl segment = new SegmentImpl(conf, file.getPath(), 0, false, new DataBlockCacheImpl(conf));

    try {
      segment.init();

      List<KVRecord> records = generateData(100);

      for (KVRecord r : records) {
        segment.put(r);
      }

      Assert.assertFalse(segment.isFull());

      segment.flush();

      for (KVRecord r : records) {
        Assert.assertNotNull(segment.get(r.getKey()));
      }
    } finally {
      segment.close();
    }
  }

  @Test
  public void test_Write_Multi_DataBlock() throws IOException {
    Conf conf = new Conf();
    conf.set(Conf.ConfVar.BLOCK_SIZE_LIMIT_MB, "1");

    SegmentImpl segment = new SegmentImpl(conf, file.getPath(), 0, false, new DataBlockCacheImpl(conf));

    try {
      segment.init();

      List<KVRecord> records = generateData(10_000);

      for (KVRecord r : records) {
        segment.put(r);
      }

      segment.flush();

      for (KVRecord r : records) {
        Assert.assertNotNull(segment.get(r.getKey()));
      }
    } finally {
      segment.close();
    }
  }
}
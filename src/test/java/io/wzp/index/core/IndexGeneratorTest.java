package io.wzp.index.core;

import io.wzp.index.io.DataBlockCacheImpl;
import io.wzp.index.io.KVRecord;
import io.wzp.index.utils.BytesHelper;
import org.junit.*;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static org.junit.Assert.*;

/**
 * @author wzp
 * @since 2019/06/16
 */
public class IndexGeneratorTest {

  @Rule
  public TemporaryFolder folder = new TemporaryFolder();

  private File dir;

  @Before
  public void setUp() throws IOException {
    dir = folder.newFolder();
  }

  @After
  public void tearDown() throws IOException {

  }

  private class KVPair {
    byte[] key;
    byte[] value;

    public KVPair(byte[] key, byte[] value) {
      this.key = key;
      this.value = value;
    }
  }

  @Test
  public void test_PutData() throws IOException, InterruptedException {
    Conf conf = new Conf();
    conf.set(Conf.ConfVar.BIG_VALUE_SIZE_LIMIT_KB, "1");
    IndexGenerator handler = new IndexGenerator(dir.getPath(), conf, new DataBlockCacheImpl(conf));

    handler.init();

    int num = 1000;
    Random random = new Random();
    List<KVPair> list = new ArrayList<>();

    for (int i = 0; i < num; i++) {
      int ranKeyLen = random.nextInt(1024) + 1;
      int ranValueLen = random.nextInt(2048) + 1;

      byte[] key = new byte[ranKeyLen];
      byte[] value = new byte[ranValueLen];

      random.nextBytes(key);
      random.nextBytes(value);

      list.add(new KVPair(key, value));
      handler.put(key, value);
    }

    handler.flush();

    KVManager kvManager = new KVManager(handler.getSegments(), handler.getValueFiles());

    for (KVPair pair : list) {
      Assert.assertArrayEquals(pair.value, kvManager.getValue(pair.key));
    }
  }
}
package io.wzp.index;

import io.wzp.index.benchmark.BenchmarkEvent;
import io.wzp.index.benchmark.BenchmarkHandler;
import io.wzp.index.core.Conf;
import io.wzp.index.core.IndexGenerator;
import io.wzp.index.core.KVManager;
import io.wzp.index.io.*;
import io.wzp.index.utils.BytesHelper;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

/**
 * @author wzp
 * @since 2019/06/15
 */

public class Main {

  private static final String O_ARG = "-o";
  private static final String D_ARG = "-d";
  private static final String T_ARG = "-t";
  private static final String N_ARG = "-n";
  private static final String B_ARG = "-b";

  private final Argument argument;
  private final Conf conf;
  private final DataBlockCache cache;


  public Main(Argument argument, Conf conf) {
    this.argument = argument;
    this.conf = conf;
    this.cache = new DataBlockCacheImpl(conf);
  }

  private void checkArguments() {
    if (argument.indexedDir == null) {
      System.err.println("Missing index directory");
      printUsage();
      System.exit(1);
    }

    if (argument.testDir == null) {
      System.err.println("Missing test directory");
      printUsage();
      System.exit(1);
    } else if (!Files.exists(new File(argument.testDir).toPath())) {
      System.err.println("Test directory not exists");
      printUsage();
      System.exit(1);
    }

    if (argument.unIndexedDir != null && !Files.exists(new File(argument.unIndexedDir).toPath())) {
      System.err.println("Unsorted data directory not exists");
      printUsage();
      System.exit(1);
    }

    if (argument.threadNum < 0) {
      System.err.println("Negative thread number");
      printUsage();
      System.exit(1);
    }

    if (argument.batchNum < 0) {
      System.err.println("Negative batch number");
      printUsage();
      System.exit(1);
    }
  }

  public void run() throws IOException, InterruptedException {
    checkArguments();

    File f = new File(argument.indexedDir);

    if (Files.notExists(f.toPath())) {
      Files.createDirectories(f.toPath());
    }

    KVManager kvManager;

    if (argument.unIndexedDir != null) {
      //clear existed index
      deleteDiskFiles(new File(argument.indexedDir));

      kvManager = buildIndex();
    } else {
      kvManager = buildKVManager();
    }

    testKV(kvManager);
  }

  /**
   * Test kv
   * @param kvManager resource handler
   * @throws IOException
   */
  private void testKV(KVManager kvManager) throws IOException, InterruptedException {
    long s = System.currentTimeMillis();

    System.out.println("Start testing kv...");

    BenchmarkHandler benchmarkHandler = new BenchmarkHandler(kvManager, argument.threadNum);

    List<byte[]> keys = new ArrayList<>();
    List<byte[]> values = new ArrayList<>();

    try {
      //read disorder data and build index
      File testDir = new File(argument.testDir);

      File[] files = testDir.listFiles();

      if (files != null) {
        for (File file : files) {
          byte[] keyLenInBytes = new byte[8];
          byte[] valueLenInBytes = new byte[8];

          System.out.println(String.format("Parse testing kv from %s...", file.getPath()));

          try (BufferedInputStream reader = new BufferedInputStream(new FileInputStream(file))) {
            while (true) {
              //key len
              boolean ret = read(keyLenInBytes, reader);

              if (!ret) {
                break;
              }

              int keyLen = (int) ByteBuffer.wrap(keyLenInBytes).getLong();

              //key
              byte[] key = new byte[keyLen];
              read(key, reader);

              //value len
              read(valueLenInBytes, reader);
              int valueLen = (int) ByteBuffer.wrap(valueLenInBytes).getLong();

              //value
              byte[] value = new byte[valueLen];
              read(value, reader);

              //Build benchmark event
              if (keys.size() == argument.batchNum) {
                benchmarkHandler.post(new BenchmarkEvent(keys, values));
                keys = new ArrayList<>();
                values = new ArrayList<>();
              }

              keys.add(key);
              values.add(value);
            }
          }
        }

        if (!keys.isEmpty()) {
          benchmarkHandler.post(new BenchmarkEvent(keys, values));
        }
      }

    } finally {
      benchmarkHandler.close();

      long searchKVCost = System.currentTimeMillis() - s;
      System.out.println(String.format("Query kv costs %d ms", searchKVCost));
    }
  }

  private void deleteDiskFiles(File parent) throws IOException {
    if (parent.exists()) {
      if (parent.isDirectory()) {
        File[] subFiles = parent.listFiles();

        if (subFiles != null) {
          for (File sub : subFiles) {
            deleteDiskFiles(sub);
          }
        }
      }

      Files.delete(parent.toPath());
    }
  }

  private KVManager buildKVManager() throws IOException {
    File segmentDir = new File(argument.indexedDir, IndexGenerator.INDEX_DIR_NAME);
    File valueFileDir = new File(argument.indexedDir, IndexGenerator.VALUE_DIR_NAME);

    if (Files.notExists(segmentDir.toPath()) || Files.notExists(valueFileDir.toPath())) {
      throw new IOException(String.format("Dir (%s) or (%s) not exists", segmentDir, valueFileDir));
    }

    String[] sfs = segmentDir.list();
    String[] vfs = valueFileDir.list();

    List<Segment> segments = new ArrayList<>(sfs.length);
    List<ValueFile> valueFiles = new ArrayList<>(vfs.length);

    for (String sf : sfs) {
      File sgPath = new File(segmentDir, sf);

      Segment segment = new SegmentImpl(conf, sgPath.getPath(), Integer.parseInt(sf), true, cache);
      segment.init();

      segments.add(segment);
    }

    Arrays.sort(vfs, (o1, o2) -> Integer.parseInt(o1) - Integer.parseInt(o2));

    for (String vf : vfs) {
      File vfPath = new File(valueFileDir, vf);

      ValueFile valueFile = new ValueFile(vfPath.getPath(), Integer.parseInt(vf), true, conf);
      valueFile.init();

      valueFiles.add(valueFile);
    }

    return new KVManager(segments, valueFiles);
  }

  private KVManager buildIndex() throws IOException, InterruptedException {
    long s = System.currentTimeMillis();

    System.out.println("Start building index...");

    //Build index generator
    IndexGenerator indexGenerator = new IndexGenerator(argument.indexedDir, conf, cache);

    indexGenerator.init();

    //read disorder data and build index
    File unIndexedDir = new File(argument.unIndexedDir);

    File[] files = unIndexedDir.listFiles();

    if (files != null) {
      for (File file : files) {
        byte[] keyLenInBytes = new byte[8];
        byte[] valueLenInBytes = new byte[8];

        System.out.println(String.format("Building index from %s...", file.getPath()));

        try (BufferedInputStream reader = new BufferedInputStream(new FileInputStream(file))) {
          while (true) {
            //key len
            boolean ret = read(keyLenInBytes, reader);

            if (!ret) {
              break;
            }

            int keyLen = (int) ByteBuffer.wrap(keyLenInBytes).getLong();

            //key
            byte[] key = new byte[keyLen];
            read(key, reader);

            //value len
            read(valueLenInBytes, reader);
            int valueLen = (int) ByteBuffer.wrap(valueLenInBytes).getLong();

            //value
            byte[] value = new byte[valueLen];
            read(value, reader);

            indexGenerator.put(key, value);
          }
        }
      }
    }

    long buildIndexCost = System.currentTimeMillis() - s;

    System.out.println(String.format("Build index costs %d ms", buildIndexCost));

    //close index generator
    indexGenerator.close();

    return new KVManager(indexGenerator.getSegments(), indexGenerator.getValueFiles());
  }

  private boolean read(byte[] dest, InputStream reader) throws IOException {
    int remaining = dest.length;

    while (remaining > 0) {
      int ret = reader.read(dest, dest.length - remaining, remaining);

      if (ret < 0) {
        return false;
      }

      remaining -= ret;
    }

    return true;
  }

  private static Argument parseArgument(String args[]) {
    String unIndexedDir = null, indexedDir = null, testDir = null;
    int threadNum = 1, batchCount = 1000;

    for (int i = 0; i < args.length; i += 2) {
      String arg = args[i].toLowerCase();

      switch (arg) {
        case O_ARG:
          unIndexedDir = args[i + 1];
          break;
        case D_ARG:
          indexedDir = args[i + 1];
          break;
        case T_ARG:
          testDir = args[i + 1];
          break;
        case N_ARG:
          threadNum = Integer.parseInt(args[i + 1]);
          break;
        case B_ARG:
          batchCount = Integer.parseInt(args[i + 1]);
          break;
        default: throw new IllegalArgumentException(String.format("Unknown param %s", args[i]));
      }
    }

    return new Argument(unIndexedDir, indexedDir, testDir, threadNum, batchCount);
  }

  private static void printUsage() {
    String errMsg = "Main [options]\n";
    errMsg += String.format("%s\t%s", O_ARG, "origin data dir\n");
    errMsg += String.format("%s\t%s", D_ARG, "indexed dir\n");
    errMsg += String.format("%s\t%s", T_ARG, "test data dir\n");
    errMsg += String.format("%s\t%s", N_ARG, "thread number\n");
    errMsg += String.format("%s\t%s", B_ARG, "batch number when testing\n");

    System.err.println(errMsg);
    System.exit(1);
  }


  public static void main(String[] args) throws IOException, InterruptedException {
    if (args.length % 2 != 0) {
      printUsage();
    }

    Argument argument = parseArgument(args);
    Conf conf = new Conf();

    Main main = new Main(argument, conf);

    main.run();
  }

  private static class Argument {
    private final String unIndexedDir;
    private final String indexedDir;
    private final String testDir;
    private final int threadNum;
    private final int batchNum;

    public Argument(String unIndexedDir, String indexedDir, String testDir, int threadNum, int batchNum) {
      this.unIndexedDir = unIndexedDir;
      this.indexedDir = indexedDir;
      this.testDir = testDir;
      this.threadNum = threadNum;
      this.batchNum = batchNum;
    }
  }
}

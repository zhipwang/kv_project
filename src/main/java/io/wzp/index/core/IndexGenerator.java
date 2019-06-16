package io.wzp.index.core;

import io.wzp.index.io.*;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

/**
 * @author wzp
 * @since 2019/06/16
 */

public class IndexGenerator {

  public static final String INDEX_DIR_NAME = "index";
  public static final String VALUE_DIR_NAME = "value";

  private final String dirPath;
  private final Path indexDirPath;
  private final Path valueDirPath;
  private final Conf conf;
  private final DataBlockCache blockCache;
  private final int bigValueSizeLimit;

  private final List<Segment> segments;
  private final List<ValueFile> valueFiles;
  private final FlushHandler flushHandler;

  private int segmentId;
  private int valueFileId;

  private ValueFile curValueFile;
  private Segment curSegment;
  private SkipList curSkipList;

  public IndexGenerator(String dirPath, Conf conf, DataBlockCache blockCache) {
    this.dirPath = dirPath;
    this.indexDirPath = new File(dirPath, INDEX_DIR_NAME).toPath();
    this.valueDirPath = new File(dirPath, VALUE_DIR_NAME).toPath();
    this.conf = conf;
    this.blockCache = blockCache;

    this.segments = new ArrayList<>();
    this.valueFiles = new ArrayList<>();

    this.segmentId = 0;
    this.valueFileId = 0;

    this.bigValueSizeLimit = 1024 * Integer.parseInt(conf.get(Conf.ConfVar.BIG_VALUE_SIZE_LIMIT_KB));

    this.flushHandler = new FlushHandler(conf);
  }

  public List<Segment> getSegments() {
    return segments;
  }

  public List<ValueFile> getValueFiles() {
    return valueFiles;
  }

  public void init() throws IOException {
    Files.createDirectories(new File(dirPath).toPath());

    if (Files.notExists(indexDirPath)) {
      Files.createDirectory(indexDirPath);
    }

    if (Files.notExists(valueDirPath)) {
      Files.createDirectory(valueDirPath);
    }

    curValueFile = buildNewValueFile();
    curSegment = buildNewSegment();
    curSkipList = new SkipListImpl(conf);
  }

  public void close() throws IOException, InterruptedException {
    flush();
  }

  private ValueFile buildNewValueFile() throws IOException {
    int s = valueFileId++;

    ValueFile result = new ValueFile(
      new File(valueDirPath.toFile(), String.format("%d", s)).getPath(),
      s,
      false,
      conf
    );

    result.init();

    return result;
  }

  private Segment buildNewSegment() throws IOException {
    int s = segmentId++;

    Segment result = new SegmentImpl(
      conf,
      new File(indexDirPath.toFile(), String.format("%d", s)).getPath(),
      s,
      false,
      blockCache
    );

    result.init();

    return result;
  }

  /**
   * Put kv pair
   * @param key key
   * @param value value
   * @throws IOException
   * @throws InterruptedException
   */
  public void put(byte[] key, byte[] value) throws IOException, InterruptedException {
    if (curSkipList.isFull()) {
      //flush mem
      flushHandler.flush(curSkipList, curSegment);

      segments.add(curSegment);

      curSkipList = new SkipListImpl(conf);
      curSegment = buildNewSegment();
    }

    if (curValueFile.isFull()) {
      //flush value file
      curValueFile.flush();

      valueFiles.add(curValueFile);

      curValueFile = buildNewValueFile();
    }


    KVRecord record;

    if (value.length > bigValueSizeLimit) {
      record = new KVRecord(key, null, false, curValueFile.getFileId(), curValueFile.getCurSize(), value.length);

      curValueFile.put(value);
    } else {
      record = new KVRecord(key, value, true, -1, -1, -1);
    }

    curSkipList.put(record);
  }

  /**
   * flush
   *
   * @throws IOException
   * @throws InterruptedException
   */
  public void flush() throws IOException, InterruptedException {
    if (!curSkipList.isEmpty()) {
      flushHandler.flush(curSkipList, curSegment);

      segments.add(curSegment);
    }

    //wait until all segments flush
    flushHandler.close();

    if (!curValueFile.isEmpty()) {
      curValueFile.flush();

      valueFiles.add(curValueFile);
    }
  }
}

package io.wzp.index.io;

import io.wzp.index.core.Conf;
import io.wzp.index.utils.BufferHelper;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/**
 * @author wzp
 * @since 2019/06/15
 */

public class SegmentImpl implements Segment {

  private final Conf conf;
  private final String filePath;
  private final int segmentId;
  private boolean isReadOnly;
  private final long segmentSizeLimit;
  private final DataBlockCache blockCache;

  private FileChannel segmentChannel;
  private RandomAccessFile segmentFile;

  private TailBlock tailBlock;
  private SegmentMetaBlock metaBlock;

  private DataBlockWriter writer;
  private OutputStream fos;

  public SegmentImpl(Conf conf,
                     String filePath,
                     int segmentId,
                     boolean isReadOnly,
                     DataBlockCache blockCache) {
    this.conf = conf;
    this.filePath = filePath;
    this.segmentId = segmentId;
    this.isReadOnly = isReadOnly;
    this.segmentSizeLimit = 1024L * 1024L * Integer.parseInt(conf.get(Conf.ConfVar.SEGMENT_SIZE_LIMIT_MB));
    this.blockCache = blockCache;
  }

  public String getFilePath() {
    return filePath;
  }

  public int getSegmentId() {
    return segmentId;
  }

  public void init() throws IOException {
    if (isReadOnly) {
      segmentFile = new RandomAccessFile(filePath, "r");
      segmentChannel = segmentFile.getChannel();

      long fileLength = segmentFile.length();

      ByteBuffer buf1 = BufferHelper.getBuffer(
        segmentChannel,
        true,
        fileLength - TailBlock.TAIL_BLOCK_SIZE,
        TailBlock.TAIL_BLOCK_SIZE
      );

      tailBlock = TailBlock.readFrom(buf1);

      ByteBuffer buf2 = BufferHelper.getBuffer(
        segmentChannel,
        true,
        tailBlock.getMetaOffset(),
        tailBlock.getMetaSize()
      );

      metaBlock = SegmentMetaBlock.readFrom(buf2);

      BufferHelper.disposeMappedByteBuffer(buf1);
      BufferHelper.disposeMappedByteBuffer(buf2);
    } else {
      segmentFile = new RandomAccessFile(filePath, "rw");
      segmentChannel = segmentFile.getChannel();

      fos = new BufferedOutputStream(new FileOutputStream(segmentFile.getFD()));

      int blockSizeLimit = 1024 * 1024 * Integer.parseInt(conf.get(Conf.ConfVar.BLOCK_SIZE_LIMIT_MB));
      int indexSpan = Integer.parseInt(conf.get(Conf.ConfVar.BLOCK_INDEX_SPAN));

      writer = new DataBlockWriter(fos, blockSizeLimit, indexSpan);

      metaBlock = new SegmentMetaBlock();
    }
  }

  public void close() throws IOException {
    segmentFile.close();
  }

  /**
   * Put data into segment
   *
   * @param record record
   * @throws IOException
   */
  public void put(KVRecord record) throws IOException {
    if (writer.isFull()) {
      SegmentMetaEntry me = writer.flush();

      if (me == null) {
        throw new IOException("Null segment meta entry");
      }

      metaBlock.add(me);
    }

    writer.put(record);
  }

  /**
   * Flush segment
   *
   * @throws IOException
   */
  public void flush() throws IOException {
    if (!writer.isEmpty()) {
      SegmentMetaEntry me = writer.flush();

      if (me == null) {
        throw new IOException("Null segment meta entry");
      }

      metaBlock.add(me);
    }

    //meta block
    long metaBlockOffset = writer.getStartOffset();

    int metaBlockSize = metaBlock.writeTo(fos);

    //tail block
    tailBlock = new TailBlock(metaBlockOffset, metaBlockSize);

    tailBlock.writeTo(fos);

    fos.flush();
    fos = null;

    //close writer
    writer = null;

    isReadOnly = true;
  }

  public boolean isFull() throws IOException {
    return isReadOnly || writer == null || writer.getStartOffset() + writer.getBlockSize() >= segmentSizeLimit;
  }

  /**
   *
   * @return first key of the block, null if empty
   */
  public byte[] getStartKey() {
    return metaBlock.getStartKey();
  }

  /**
   *
   * @return last key of the block, null if empty
   */
  public byte[] getEndKey() {
    return metaBlock.getEndKey();
  }

  /**
   *
   * @param targetKey key
   * @return KV record, null if no key matches
   * @throws IOException
   */
  public KVRecord get(byte[] targetKey) throws IOException {
    SegmentMetaEntry entry = metaBlock.getEntry(targetKey);

    if (entry == null) {
      return null;
    }

    long cacheId = ((long) segmentId) << 32 | entry.getBlockId();

    DataBlock candidateBlock = blockCache.get(cacheId);

    if (candidateBlock != null) {
      return candidateBlock.get(targetKey);
    }

    ByteBuffer buf = BufferHelper.getBuffer(
      segmentChannel,
      true,
      entry.getBlockStartOffset(),
      entry.getBlockSize()
    );

    DataBlock fetchedBlock = new DataBlock(buf);

    fetchedBlock.init();

    KVRecord result = fetchedBlock.get(targetKey);

    blockCache.put(cacheId, fetchedBlock);

    return result;
  }
}

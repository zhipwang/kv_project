package io.wzp.index.io;

import io.wzp.index.core.Conf;
import io.wzp.index.utils.BufferHelper;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/**
 * @author wzp
 * @since 2019/06/16
 */

public class ValueFile {

  private final String filePath;
  private final int fileId;
  private boolean isReadOnly;
  private final Conf conf;
  private final long valueFileSizeLimit;

  private RandomAccessFile file;
  private FileChannel channel;

  private OutputStream fos;
  private long curSize;

  public ValueFile(String filePath, int fileId, boolean isReadOnly, Conf conf) {
    this.filePath = filePath;
    this.fileId = fileId;
    this.isReadOnly = isReadOnly;
    this.conf = conf;
    this.valueFileSizeLimit = 1024L * 1024L * Integer.parseInt(conf.get(Conf.ConfVar.VALUE_FILE_SIZE_LIMIT_MB));
    this.curSize = 0;
  }

  public int getFileId() {
    return fileId;
  }

  public void init() throws IOException {
    if (isReadOnly) {
      file = new RandomAccessFile(filePath, "r");
      channel = file.getChannel();
    } else {
      file = new RandomAccessFile(filePath, "rw");
      channel = file.getChannel();

      fos = new BufferedOutputStream(new FileOutputStream(file.getFD()));
    }
  }

  public void close() throws IOException {
    file.close();
  }

  public void put(byte[] value) throws IOException {
    fos.write(value);

    curSize += value.length;
  }

  public long getCurSize() {
    return curSize;
  }

  public boolean isEmpty() {
    return curSize == 0;
  }

  public boolean isFull() {
    return isReadOnly || curSize >= valueFileSizeLimit;
  }

  public void flush() throws IOException {
    fos.flush();
    fos = null;

    isReadOnly = true;
  }

  public byte[] get(long offset, int size) throws IOException {
    ByteBuffer buffer = BufferHelper.getBuffer(channel, true, offset, size);

    byte[] result = new byte[size];
    buffer.get(result);

    BufferHelper.disposeMappedByteBuffer(buffer);

    return result;
  }
}

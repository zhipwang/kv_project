package io.wzp.index;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.util.Random;

/**
 * @author wzp
 * @since 2019/06/16
 */

public class DataGenerator {

  private static final String D_ARG = "-d";
  private static final String S_ARG = "-s";
  private static final String L_ARG = "-l";

  private static final int KEY_SIZE_LIMIT = 1024;
  private static final int VALUE_SIZE_LIMIT = 1024 * 1024;

  private static void printUsage() {
    String errMsg = "DataGenerator [options]\n";
    errMsg += String.format("%s\t%s", "-d", "data target dir\n");
    errMsg += String.format("%s\t%s", "-s", "data size in MB\n");
    errMsg += String.format("%s\t%s", "-l", "single file size limit in MB\n");

    System.err.println(errMsg);
    System.exit(1);
  }

  private static Argument parseArgument(String[] args) {
    String dataDir = null;
    int dataSizeInMB = -1, singleFileSizeLimitInMB = -1;

    for (int i = 0; i < args.length; i += 2) {
      switch (args[i].toLowerCase()) {
        case D_ARG: dataDir = args[i + 1]; break;

        case S_ARG: dataSizeInMB = Integer.parseInt(args[i + 1]); break;

        case L_ARG: singleFileSizeLimitInMB = Integer.parseInt(args[i + 1]); break;

        default: throw new IllegalArgumentException(String.format("Unknown argument %s", args[i]));
      }
    }

    return new Argument(dataDir, dataSizeInMB, singleFileSizeLimitInMB);
  }

  public static void main(String[] args) throws IOException {
    if (args.length % 2 != 0) {
      printUsage();
    }

    Argument argument = parseArgument(args);

    if (argument.dataDir == null) {
      throw new IllegalArgumentException("Data dir not provided");
    }

    if (argument.dataSizeInMB < 0) {
      throw new IllegalArgumentException("Invalid data size found");
    }

    if (argument.singleFileSizeLimitInMB < 0) {
      throw new IllegalArgumentException("Invalid single file size found");
    }

    //data dir
    File dataDir = new File(argument.dataDir);

    Files.createDirectories(dataDir.toPath());

    //gen data
    long totalDataSizeLimit = 1024L * 1024L * argument.dataSizeInMB;
    long singleFileSizeLimit = 1024L * 1024L * argument.singleFileSizeLimitInMB;

    buildData(dataDir, totalDataSizeLimit, singleFileSizeLimit);
  }

  private static void buildData(File dataDir,
                                long totalDataSizeLimit,
                                long singleFileSizeLimit) throws IOException {
    int fileNum = 0;
    long curTotalDataSize = 0;
    long curSingleFileSize = 0;

    Random random = new Random();

    BufferedOutputStream out = new BufferedOutputStream(new FileOutputStream(
      new File(dataDir, String.format("%d.data", fileNum++))
    ));

    try {
      while (curTotalDataSize < totalDataSizeLimit) {
        int ranKeyLen = random.nextInt(KEY_SIZE_LIMIT) + 1;
        int ranValueLen = random.nextInt(VALUE_SIZE_LIMIT) + 1;

        byte[] ranKey = new byte[ranKeyLen];
        byte[] ranValue = new byte[ranValueLen];

        random.nextBytes(ranKey);
        random.nextBytes(ranValue);

        if (curSingleFileSize >= singleFileSizeLimit) {
          out.close();
          curSingleFileSize = 0;

          out = new BufferedOutputStream(new FileOutputStream(
            new File(dataDir, String.format("%d.data", fileNum++))
          ));
        }

        //write key len
        ByteBuffer buf1 = ByteBuffer.allocate(8);
        buf1.putLong(ranKeyLen);
        out.write(buf1.array());

        //write key
        out.write(ranKey);

        //write value len
        ByteBuffer buf2 = ByteBuffer.allocate(8);
        buf2.putLong(ranValueLen);
        out.write(buf2.array());

        //write value
        out.write(ranValue);

        curTotalDataSize += 16 + ranKeyLen + ranValueLen;
        curSingleFileSize += 16 + ranKeyLen + ranValueLen;
      }
    } finally {
      out.close();
    }
  }


  private static class Argument {
    private final String dataDir;
    private final int dataSizeInMB;
    private final int singleFileSizeLimitInMB;

    public Argument(String dataDir, int dataSizeInMB, int singleFileSizeLimitInMB) {
      this.dataDir = dataDir;
      this.dataSizeInMB = dataSizeInMB;
      this.singleFileSizeLimitInMB = singleFileSizeLimitInMB;
    }
  }
}

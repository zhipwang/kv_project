package io.wzp.index.benchmark;

import io.wzp.index.core.KVManager;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

/**
 * @author wzp
 * @since 2019/06/16
 */

public class BenchmarkHandler {

  private final BlockingQueue<BenchMarkThread> queue;
  private final BenchMarkThread[] threads;

  public BenchmarkHandler(KVManager kvManager, int threadNum) {
    this.queue = new ArrayBlockingQueue<>(threadNum);
    this.threads = new BenchMarkThread[threadNum];

    for (int i = 0; i < threadNum; i++) {
      threads[i] = new BenchMarkThread(kvManager, queue, i);
      threads[i].start();
      queue.add(threads[i]);
    }
  }

  public void close() throws InterruptedException {
    long totalInvalidCount = 0;

    for (BenchMarkThread thread : threads) {
      thread.close();
      thread.join();

      totalInvalidCount += thread.getInEqualCount();
    }

    System.out.println(String.format("KV not match count: %d", totalInvalidCount));
  }

  public void post(BenchmarkEvent event) throws InterruptedException {
    BenchMarkThread thread = queue.take();

    thread.post(event);
  }
}

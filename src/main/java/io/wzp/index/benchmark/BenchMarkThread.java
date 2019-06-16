package io.wzp.index.benchmark;

import io.wzp.index.core.KVManager;
import io.wzp.index.utils.BytesHelper;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author wzp
 * @since 2019/06/16
 */

public class BenchMarkThread extends Thread {
  private final KVManager kvManager;
  private final AtomicBoolean running;
  private final BlockingQueue<BenchMarkThread> queue;
  private final Semaphore lock;
  private final BlockingQueue<BenchmarkEvent> events;

  private long inEqualCount = 0;

  public BenchMarkThread(KVManager kvManager, BlockingQueue<BenchMarkThread> queue, int id) {
    this.kvManager = kvManager;
    this.queue = queue;
    this.running = new AtomicBoolean(true);
    this.lock = new Semaphore(0);
    this.events = new ArrayBlockingQueue<>(1);

    setName(String.format("BENCHMARK_%d", id));
  }

  public void close() {
    running.compareAndSet(true, false);
    post(null);
  }

  public void post(BenchmarkEvent event) {
    if (event != null) {
      events.offer(event);
    }

    lock.release();
  }

  public long getInEqualCount() {
    return inEqualCount;
  }

  @Override
  public void run() {
    try {
      while (running.get()) {
        lock.acquire();

        BenchmarkEvent flushEvent = events.poll();

        if (flushEvent != null) {
          doQuery(flushEvent);
        }

        queue.offer(this);
      }
    } catch (Throwable t) {
      t.printStackTrace();
    }
  }

  private void doQuery(BenchmarkEvent event) throws IOException {
    List<byte[]> keys = event.getKeys();
    List<byte[]> values = event.getResult();

    for (int i = 0; i < keys.size(); i++) {
      byte[] candidate = kvManager.getValue(keys.get(i));

      if (candidate != null) {
        //check result
        int cmp = BytesHelper.compareByteArray(candidate, values.get(i));

        if (cmp != 0) {
          inEqualCount++;
        }
      }
    }
  }
}

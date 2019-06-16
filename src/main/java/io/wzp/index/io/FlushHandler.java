package io.wzp.index.io;

import io.wzp.index.core.Conf;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

/**
 * @author wzp
 * @since 2019/06/16
 */

public class FlushHandler {

  private final BlockingQueue<FlushThread> queue;
  private final FlushThread[] threads;

  public FlushHandler(Conf conf) {
    int threadNum = Integer.parseInt(conf.get(Conf.ConfVar.FLUSH_THREAD_NUM));
    this.queue = new ArrayBlockingQueue<>(threadNum);

    this.threads = new FlushThread[threadNum];

    for (int i = 0; i < threadNum; i++) {
      threads[i] = new FlushThread(i, queue);
      threads[i].start();
      queue.add(threads[i]);
    }
  }

  public void close() throws InterruptedException {
    for (FlushThread thread : threads) {
      thread.close();
      thread.join();
    }

    queue.clear();
  }

  public void flush(SkipList skipList, Segment segment) throws InterruptedException {
    FlushThread thread = queue.take();

    thread.post(new FlushEvent(segment, skipList));
  }
}

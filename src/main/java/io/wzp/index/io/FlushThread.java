package io.wzp.index.io;

import java.io.IOException;
import java.util.Iterator;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author wzp
 * @since 2019/06/16
 */

public class FlushThread extends Thread {

  private final AtomicBoolean running;
  private final BlockingQueue<FlushThread> queue;
  private final Semaphore lock;
  private final BlockingQueue<FlushEvent> events;

  public FlushThread(int id, BlockingQueue<FlushThread> queue) {
    this.running = new AtomicBoolean(true);
    this.queue = queue;
    this.lock = new Semaphore(0);
    this.events = new ArrayBlockingQueue<>(1);

    setName(String.format("FLUSH_%d", id));
  }

  public void close() {
    running.compareAndSet(true, false);
    post(null);
  }

  public void post(FlushEvent event) {
    if (event != null) {
      events.offer(event);
    }

    lock.release();
  }

  @Override
  public void run() {
    try {
      while (running.get()) {
        lock.acquire();

        FlushEvent flushEvent = events.poll();

        if (flushEvent != null) {
          doFlush(flushEvent);
        }

        queue.offer(this);
      }
    } catch (Throwable t) {
      t.printStackTrace();
    }
  }


  private void doFlush(FlushEvent flushEvent) throws IOException {
    Segment segment = flushEvent.getSegment();
    SkipList skipList = flushEvent.getSkipList();

    Iterator<KVRecord> iterator = skipList.toIterator();

    while (iterator.hasNext()) {
      segment.put(iterator.next());
    }

    segment.flush();
    skipList.reset();
  }
}

package io.wzp.index.io;

import junit.framework.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.Iterator;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

/**
 * @author wzp
 * @since 2019/06/16
 */
public class FlushThreadTest {

  @Test
  public void test() throws IOException, InterruptedException {
    BlockingQueue<FlushThread> queue = new ArrayBlockingQueue<FlushThread>(2);

    FlushThread thread = new FlushThread(0, queue);

    thread.start();

    SkipList mockSkipList = mock(SkipList.class);

    doNothing().when(mockSkipList).reset();

    when(mockSkipList.toIterator()).thenReturn(
      new Iterator<KVRecord>() {

        @Override
        public boolean hasNext() {
          return false;
        }

        @Override
        public KVRecord next() {
          return null;
        }
      });

    Segment mockSegment = mock(Segment.class);

    doNothing().when(mockSegment).flush();


    //send event
    thread.post(new FlushEvent(mockSegment, mockSkipList));

    Thread.sleep(1000);

    verify(mockSegment).flush();
    verify(mockSkipList).toIterator();

    thread.close();

    Assert.assertEquals(1, queue.size());
  }
}
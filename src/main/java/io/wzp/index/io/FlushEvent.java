package io.wzp.index.io;

/**
 * @author wzp
 * @since 2019/06/16
 */

public class FlushEvent {

  private final Segment segment;
  private final SkipList skipList;

  public FlushEvent(Segment segment, SkipList skipList) {
    this.segment = segment;
    this.skipList = skipList;
  }

  public Segment getSegment() {
    return segment;
  }

  public SkipList getSkipList() {
    return skipList;
  }
}

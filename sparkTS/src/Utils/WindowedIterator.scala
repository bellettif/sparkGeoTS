package Utils

import scala.Iterator
import scala.Seq
import scala.collection.mutable.ArrayBuffer
import scala.collection.{Seq, Iterator}

/**
 * Created by Francois Belletti on 7/7/15.
 */
trait WindowedIterator[+A] extends Iterator[A]{
  self =>

  /** A flexible iterator for transforming an `Iterator[A]` into an
    *  Iterator[Seq[A]], with configurable sequence size, step, and
    *  strategy for dealing with elements which don't fit evenly.
    *
    *  Typical uses can be achieved via methods `grouped' and `sliding'.
    */
  class WindowedGroupedIterator[B >: A](self: Iterator[A], windowIdxs: Iterator[(Int, Int)])
    extends Iterator[Seq[B]] {

    val nWindows = windowIdxs.size
    for((startIdx, stopIdx) <- windowIdxs){
      require(stopIdx < startIdx, "Stop index (%d) must be > start index (%d)".format(startIdx, stopIdx))
    }

    private[this] var buffer: Seq[B] = Nil  // the buffer
    private[this] var lastIdx: Int = 0
    private[this] var filled = false
    private[this] val iter = windowIdxs

    /** For reasons which remain to be determined, calling
      *  self.take(n).toSeq cause an infinite loop, so we have
      *  a slight variation on take for local usage.
      */
    private def takeDestructively(size: Int): Seq[A] = {
      val buf = new ArrayBuffer[A]
      var i = 0
      while (self.hasNext && i < size) {
        buf += self.next
        i += 1
      }
      buf
    }

    private def go(startIdx: Int, stopIdx: Int): Boolean = {
      val toTrim = startIdx - lastIdx
      val toTake = stopIdx - lastIdx
      def isFirst = buffer.isEmpty

      buffer = takeDestructively(toTake)
      if(buffer.length < toTake) return false

      buffer.drop(toTrim) // Supports gaps between consecutive windows
      lastIdx = stopIdx + 1
      true
    }

    // fill() returns false if no more sequences can be produced
    private def fill(): Boolean = {
      if (!self.hasNext) false  // No values remaining
      if (!iter.hasNext) false  // No window remaining
      // the first time we grab size, but after that we grab step
      val (startIdx, stopIdx) = iter.next()
      filled = go(startIdx, stopIdx)
      filled
    }

    def hasNext = filled || fill()
    def next = {
      if (!filled)
        fill()

      if (!filled)
        throw new NoSuchElementException("next on empty iterator")
      filled = false
      buffer.toList
    }
  }



  /** Returns an iterator which presents a "sliding window" view of
    *  another iterator.  The first argument is the window size, and
    *  the second is how far to advance the window on each iteration;
    *  defaults to 1.  Example usages:
    *
    *  <pre>
    *    // Returns List(List(1, 2, 3), List(2, 3, 4), List(3, 4, 5))
    *    (1 to 5).iterator.sliding(3).toList
    *    // Returns List(List(1, 2, 3, 4), List(4, 5))
    *    (1 to 5).iterator.sliding(4, 3).toList
    *    // Returns List(List(1, 2, 3, 4))
    *    (1 to 5).iterator.sliding(4, 3).withPartial(false).toList
    *    // Returns List(List(1, 2, 3, 4), List(4, 5, 20, 25))
    *    // Illustrating that withPadding's argument is by-name.
    *    val it2 = Iterator.iterate(20)(_ + 5)
    *    (1 to 5).iterator.sliding(4, 3).withPadding(it2.next).toList
    *  </pre>
    */
  def windows[B >: A](windowsIdxs: Iterator[(Int, Int)]): WindowedGroupedIterator[B] =
    new WindowedGroupedIterator[B](self, windowsIdxs)


}

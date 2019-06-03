package PhenixChallenge.combine

trait Combinable[T] {
  def combine(other: T): T;
}

class IteratorCombinator[T <: Combinable[T]](_xs: Iterator[T], _ys: Iterator[T])(implicit ord: Ordering[T])
extends Iterator[T] {
  private val xs = _xs.buffered
  private val ys = _ys.buffered

  def hasNext = xs.hasNext || ys.hasNext

  def next() = (xs.hasNext, ys.hasNext) match {
    case (true, true) => {
      val x = xs.head
      val y = ys.head
      val comp = ord.compare(x, y)
      if (comp < 0) {
        xs.next()
      } else if (comp > 0) {
        ys.next()
      } else {
        xs.next()
        ys.next()
        x.combine(y)
      }
    }
    case (true, false) => xs.next
    case (false, true) => ys.next
    case (false, false) => ys.next // This should throw an error
  }
}

object IteratorCombinator {
  def combine[T <: Combinable[T]](xs: Iterator[T], ys: Iterator[T])(implicit ord: Ordering[T]) = {
    new IteratorCombinator(xs, ys)
  }
}

class IteratorKMerger[T](_xs: Iterator[T], _ys: Iterator[T])(implicit ord: Ordering[T])
extends Iterator[T] {
  private val xs = _xs.buffered
  private val ys = _ys.buffered

  def hasNext = xs.hasNext || ys.hasNext

  def next() = (xs.hasNext, ys.hasNext) match {
    case (true, true) => {
      val x = xs.head
      val y = ys.head
      val comp = ord.compare(x, y)
      if (comp <= 0) {
        xs.next()
      } else {
        ys.next()
      }
    }
    case (true, false) => xs.next
    case (false, true) => ys.next
    case (false, false) => ys.next // This should throw an error
  }
}

package com.twitter.algebird.util.summer

import java.util.concurrent.atomic.AtomicLong

/**
 * @author Mansur Ashraf.
 */
case class Counter(name: String) extends Incrementor {
  private val counter = new AtomicLong()

  override def incr: Unit = counter.incrementAndGet()

  override def incrBy(amount: Long): Unit = counter.addAndGet(amount)

  def size = counter.get()

  override def toString: String = s"$name: size:$size"
}

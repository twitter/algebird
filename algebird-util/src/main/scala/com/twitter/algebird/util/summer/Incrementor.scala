package com.twitter.algebird.util.summer

/**
 * @author Mansur Ashraf.
 */
trait Incrementor {
  def incr(): Unit
  def incrBy(amount: Long): Unit
}

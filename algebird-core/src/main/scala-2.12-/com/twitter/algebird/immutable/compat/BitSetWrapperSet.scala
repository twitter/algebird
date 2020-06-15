package com.twitter.algebird.immutable.compat

import com.twitter.algebird.immutable.BitSet

class BitSetWrapperSet(bitset: BitSet) extends Set[Int] {
  def contains(i: Int): Boolean = bitset(i)
  def iterator: Iterator[Int] = bitset.iterator
  def +(i: Int): BitSetWrapperSet = new BitSetWrapperSet(bitset + i)
  def -(i: Int): BitSetWrapperSet = new BitSetWrapperSet(bitset - i)
  override def empty: Set[Int] = BitSet.Empty.toSet
}

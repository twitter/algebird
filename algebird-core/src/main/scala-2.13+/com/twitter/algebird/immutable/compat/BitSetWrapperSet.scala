package com.twitter.algebird.immutable
package compat

class BitSetWrapperSet(bitset: BitSet) extends Set[Int] {
  def contains(i: Int): Boolean = bitset(i)
  def iterator: Iterator[Int] = bitset.iterator
  def incl(i: Int): BitSetWrapperSet = new BitSetWrapperSet(bitset + i)
  def excl(i: Int): BitSetWrapperSet = new BitSetWrapperSet(bitset - i)
  override def empty: Set[Int] = BitSet.Empty.toSet
}

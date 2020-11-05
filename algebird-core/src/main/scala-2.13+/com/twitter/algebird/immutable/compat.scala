package com.twitter.algebird.immutable

private[algebird] object compat {
  class BitSetWrapperSet(bitset: BitSet) extends Set[Int] {
    override def contains(i: Int): Boolean = bitset(i)

    override def iterator: Iterator[Int] = bitset.iterator

    override def incl(i: Int): BitSetWrapperSet = new BitSetWrapperSet(bitset + i)

    override def excl(i: Int): BitSetWrapperSet = new BitSetWrapperSet(bitset - i)

    override def empty: Set[Int] = BitSet.empty.toSet
  }
}

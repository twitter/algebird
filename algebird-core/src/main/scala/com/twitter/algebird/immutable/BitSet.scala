/*
Copyright 2020 Twitter, Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
 */
package com.twitter.algebird.immutable

import java.lang.Long.bitCount
import scala.annotation.tailrec

import BitSet.{Branch, Empty, Leaf}

// This implementation uses a lot of tricks for performance, which
// scalastyle is not always happy with. So we disable it.
//
// scalastyle:off

/**
 * A fast, immutable BitSet.
 *
 * This implementation is taken from cats-collections.
 * https://github.com/typelevel/cats-collections/blob/0336992942aba9aba4a322b629447fcabe251920/core/src/main/scala/cats/collections/BitSet.scala
 *
 * A Bitset is a specialized type of set that tracks the `Int` values
 * it contains: for each integer value, a BitSet uses a single bit to
 * track whether the value is present (1) or absent (0). Bitsets are
 * often sparse, since "missing" bits can be assumed to be zero.
 *
 * Unlike scala's default immutable this BitSet does not do a full
 * copy on each added value.
 *
 * Internally the implementation is a tree. Each leaf uses an
 * Array[Long] value to hold up to 2048 bits, and each branch uses an
 * Array[BitSet] to hold up to 32 subtrees (null subtrees are treated
 * as empty).
 *
 * Bitset treats the values it stores as 32-bit unsigned values, which
 * is relevant to the internal addressing methods as well as the order
 * used by `iterator`.
 *
 * The benchmarks suggest this bitset is MUCH faster than Scala's
 * built-in bitset for cases where you may need many modifications and
 * merges, (for example in a BloomFilter).
 *
 */
sealed abstract class BitSet { lhs =>

  /**
   * Offset is the first value that this subtree contains.
   *
   * Offset will always be a multiple of 2048 (2^11).
   *
   * The `offset` is interpreted as a 32-bit unsigned integer. In
   * other words, `(offset & 0xffffffffL)` will return the equivalent
   * value as a signed 64-bit integer (between 0 and 4294967295).
   */
  private[algebird] def offset: Int

  /**
   * Limit is the first value beyond the range this subtree
   * supports.
   *
   * In other words, the last value in the subtree's range is `limit - 1`.
   * Like `offset`, `limit` will always be a multiple of 2048.
   *
   * Offset, limit, and height are related:
   *
   *     limit = offset + (32^height) * 2048
   *     limit > offset (assuming both values are unsigned)
   *
   * Like `offset`, `limit` is interpreted as a 32-bit unsigned
   * integer.
   */
  private[algebird] def limit: Long

  /**
   * Height represents the number of "levels" this subtree contains.
   *
   * For leaves, height is zero. For branches, height will always be
   * between 1 and 5. This is because a branch with offset=0 and
   * height=5 will have limit=68719476736, which exceeds the largest
   * unsigned 32-bit value we might want to store (4294967295).
   *
   * The calculation `(32^height) * 2048` tells you how many values a
   * subtree contains (i.e. how many bits it holds).
   */
  private[algebird] def height: Int

  /**
   * Look for a particular value in the bitset.
   *
   * Returns whether this value's bit is set.
   */
  def apply(n: Int): Boolean

  /**
   * Return a bitset that contains `n` and whose other values are
   * identical to this one's. If this bitset already contains `n` then this
   * method does nothing.
   */
  def +(n: Int): BitSet

  /**
   * Return a bitset that does not contain `n` and whose other values
   * are identical to this one's. If this bitset does not contain `n`
   * then this method does nothing.
   */
  def -(n: Int): BitSet

  /**
   * Return the union of two bitsets as a new immutable bitset.
   *
   * If either bitset contains a given value, the resulting bitset
   * will also contain it.
   */
  def |(rhs: BitSet): BitSet

  /**
   * Return the intersection of two bitsets as a new immutable bitset.
   *
   * The resulting bitset will only contain a value if that value is
   * present in both input bitsets.
   */
  def &(rhs: BitSet): BitSet

  /**
   * Returns whether the two bitsets intersect or not.
   *
   * Equivalent to (x & y).nonEmpty but faster.
   */
  def intersects(rhs: BitSet): Boolean

  /**
   * Return the exclusive-or of two bitsets as a new immutable bitset.
   */
  def ^(rhs: BitSet): BitSet

  /**
   * Return this bitset minus the bits contained in the other bitset
   * as a new immutable bitset.
   *
   * The resulting bitset will contain exactly those values which do
   * appear in the left-hand side but do not appear in the right-hand
   * side.
   *
   * If the bitsets do not intersect, the left-hand side will be
   * returned.
   */
  def --(rhs: BitSet): BitSet

  // Internal mutability
  //
  // The following three methods (`+=`, `-=`, and `mutableAdd`) all
  // potentially mutate `this`.
  //
  // These methods are used internally by BitSet's public methods to
  // mutate newly-constructed trees before returning them to the
  // caller. This allows us to avoid unnecessary allocations when we
  // are doing a high-level operation which may result in many
  // separate modifications.

  /**
   * Add a single value `n` to this bitset.
   *
   * This method modifies this bitset. We require that the value `n`
   * is in this node's range (i.e. `offset <= n < limit`).
   */
  private[algebird] def +=(n: Int): Unit

  /**
   * Add all values from `rhs` to this bitset.
   *
   * This method modifies this bitset. We require that `this` and
   * `rhs` are aligned (i.e. they both must have the same `offset` and
   * `height`).
   */
  private[algebird] def |=(rhs: BitSet): Unit

  /**
   * Add a single value `n` to this bitset to this bitset or to the
   * smallest valid bitset that could contain it.
   *
   * Unlike `+=` this method can be called with `n` outside of this
   * node's range. If the value is in range, the method is equivalent
   * to `+=` (and returns `this`). Otherwise, it wraps `this` in new
   * branches until the node's range is large enough to contain `n`,
   * then adds the value to that node, and returns it.
   */
  private[algebird] def mutableAdd(n: Int): BitSet

  private[algebird] def mutableAdd(ns: Array[Int]): BitSet = {
    var bs = this
    var i = 0
    while (i < ns.length) {
      bs = bs.mutableAdd(ns(i))
      i += 1
    }
    bs
  }

  /**
   * Return a compacted bitset containing the same values as this one.
   *
   * This method is used to prune out "empty" branches that don't
   * contain values. By default, bitset does not try to remove empty
   * leaves when removing values (since repeatedly checking for this
   * across many deletions would be expensive).
   *
   * The bitset returned will have the same values as the current
   * bitset, but is guaranteed not to contain any empty branches.
   * Empty branches are not usually observable but would result in
   * increased memory usage.
   */
  def compact: BitSet = {
    def recur(x: BitSet): BitSet =
      x match {
        case leaf @ Leaf(_, _) =>
          if (leaf.isEmpty) null else leaf
        case Branch(o, h, cs0) =>
          var i = 0
          var found: BitSet = null
          while (i < 32 && found == null) {
            val c = cs0(i)
            if (c != null) found = recur(c)
            i += 1
          }
          if (found == null) {
            null
          } else {
            val cs1 = new Array[BitSet](32)
            cs1(i - 1) = found
            while (i < 32) {
              val c = cs0(i)
              if (c != null) cs1(i) = recur(c)
              i += 1
            }
            Branch(o, h, cs1)
          }
      }
    val res = recur(this)
    if (res == null) Empty else res
  }

  /**
   * Returns the number of distinct values in this bitset.
   *
   * For branches, this method will return the sum of the sizes of all
   * its subtrees. For leaves it returns the number of bits set in the
   * leaf (i.e. the number of values the leaf contains).
   */
  def size: Long

  /**
   * Iterate across all values in the bitset.
   *
   * Values in the iterator will be seen in "unsigned order" (e.g. if
   * present, -1 will always come last). Here's an abbreviated view of
   * this order in practice:
   *
   *   0, 1, 2, ... 2147483646, 2147483647, -2147483648, -2147483647, ... -1
   *
   * (This "unsigned order" is identical to the tree's internal order.)
   */
  def iterator: Iterator[Int]

  /**
   * Iterate across all values in the bitset in reverse order.
   *
   * The order here is exactly the reverse of `.iterator`.
   */
  def reverseIterator: Iterator[Int]

  /**
   * Returns false if this bitset contains values, true otherwise.
   */
  def isEmpty: Boolean

  /**
   * Returns true if this bitset contains values, false otherwise.
   */
  def nonEmpty: Boolean = !isEmpty

  /**
   * Produce a string representation of this BitSet.
   *
   * This representation will contain all the values in the bitset.
   * For large bitsets, this operation may be very expensive.
   */
  override def toString: String =
    iterator.map(_.toString).mkString("BitSet(", ", ", ")")

  /**
   * Produce a structured representation of this BitSet.
   *
   * This representation is for internal-use only. It gives a view of
   * how the bitset is encoded in a tree, showing leaves and branches.
   */
  private[algebird] def structure: String =
    // This is for debugging, we don't care about coverage here
    // $COVERAGE-OFF$
    this match {
      case Branch(o, h, cs) =>
        val s = cs.iterator.zipWithIndex
          .filter { case (c, _) => c != null }
          .map { case (c, i) => s"$i -> ${c.structure}" }
          .mkString("Array(", ", ", ")")
        s"Branch($o, $h, $s)"
      case Leaf(o, vs) =>
        val s = vs.zipWithIndex
          .collect {
            case (n, i) if n != 0 => s"$i -> $n"
          }
          .mkString("{", ", ", "}")
        s"Leaf($o, $s)"
    }
  // $COVERAGE-ON$

  /**
   * Universal equality.
   *
   * This method will only return true if the right argument is also a
   * `BitSet`. It does not attempt to coerce either argument in any
   * way (unlike Scala collections, for example).
   *
   * Two bitsets can be equal even if they have different underlying
   * tree structure. (For example, one bitset's tree may have empty
   * branches that the other lacks.)
   */
  override def equals(that: Any): Boolean =
    that match {
      case t: BitSet =>
        val it0 = this.iterator
        val it1 = t.iterator
        while (it0.hasNext && it1.hasNext) {
          if (it0.next() != it1.next()) return false
        }
        it0.hasNext == it1.hasNext
      case _ =>
        false
    }

  /**
   * Universal hash code.
   *
   * Bitsets that are the equal will hash to the same value. As in
   * `equals`, the values present determine the hash code, as opposed
   * to the tree structure.
   */
  override def hashCode: Int = {
    var hash: Int = 1500450271 // prime number
    val it = iterator
    while (it.hasNext) {
      hash = (hash * 1023465798) + it.next() // prime number
    }
    hash
  }

}

object BitSet {

  /**
   * Returns an empty immutable bitset.
   */
  final def empty: BitSet = Empty

  /**
   * Singleton value representing an empty bitset.
   */
  final val Empty: BitSet =
    newEmpty(0)

  /**
   * Returns an empty leaf.
   *
   * This is used internally with the assumption that it will be
   * mutated to "add" values to it. In cases where no values need to
   * be added, `empty` should be used instead.
   */
  @inline private[algebird] def newEmpty(offset: Int): BitSet =
    Leaf(offset, new Array[Long](32))

  /**
   * Construct an immutable bitset from the given integer values.
   */
  final def apply(xs: Int*): BitSet =
    if (xs.isEmpty) Empty
    else {
      var bs = newEmpty(0)
      val iter = xs.iterator
      while (iter.hasNext) {
        bs = bs.mutableAdd(iter.next())
      }
      bs
    }

  final def apply(xs: Array[Int]): BitSet =
    if (xs.length == 0) Empty
    else {
      var bs = newEmpty(0)
      var idx = 0
      while (idx < xs.length) {
        bs = bs.mutableAdd(xs(idx))
        idx += 1
      }
      bs
    }

  /**
   * Given a value (`n`), and offset (`o`) and a height (`h`), compute
   * the array index used to store the given value's bit.
   */
  @inline private[algebird] def index(n: Int, o: Int, h: Int): Int =
    (n - o) >>> (h * 5 + 6)

  case class InternalError(msg: String) extends Exception(msg)

  /**
   * Construct a parent for the given bitset.
   *
   * The parent is guaranteed to be correctly aligned, and to have a
   * height one greater than the given bitset.
   */
  private[algebird] def parentFor(b: BitSet): BitSet = {
    val h = b.height + 1
    val o = b.offset & -(1 << (h * 5 + 11))
    val cs = new Array[BitSet](32)
    val i = (b.offset - o) >>> (h * 5 + 6)
    cs(i) = b
    Branch(o, h, cs)
  }

  /**
   * Return a branch containing the given bitset `b` and value `n`.
   *
   * This method assumes that `n` is outside of the range of `b`. It
   * will return the smallest branch that contains both `b` and `n`.
   */
  @tailrec
  private def adoptedPlus(b: BitSet, n: Int): Branch = {
    val h = b.height + 1
    val o = b.offset & -(1 << (h * 5 + 11))
    val cs = new Array[BitSet](32)
    val parent = Branch(o, h, cs)
    val i = (b.offset - o) >>> (h * 5 + 6)
    // this looks unsafe since we are going to mutate parent which points
    // to b, but critically we never mutate the Array containing b
    cs(i) = b
    val j = BitSet.index(n, o, h)
    if (j < 0 || 32 <= j) {
      adoptedPlus(parent, n)
    } else {
      parent += n
      parent
    }
  }

  /**
   * Return a branch containing the given bitsets `b` and `rhs`.
   *
   * This method assumes that `rhs` is at least partially-outside of
   * the range of `b`. It will return the smallest branch that
   * contains both `b` and `rhs`.
   */
  @tailrec
  private def adoptedUnion(b: BitSet, rhs: BitSet): BitSet = {
    val h = b.height + 1
    val o = b.offset & -(1 << (h * 5 + 11))
    val cs = new Array[BitSet](32)
    val parent = Branch(o, h, cs)
    val i = (b.offset - o) >>> (h * 5 + 6)
    cs(i) = b
    val j = BitSet.index(rhs.offset, o, h)
    if (j < 0 || 32 <= j || rhs.height > parent.height) {
      adoptedUnion(parent, rhs)
    } else {
      // we don't own parent, because it points to b
      // so we can't use mutating union here:
      // If we can be sure that b and rhs don't share structure that will be mutated
      // then we could mutate:
      // parent |= rhs
      // parent
      parent | rhs
    }
  }

  private case class Branch(offset: Int, height: Int, children: Array[BitSet]) extends BitSet {

    @inline private[algebird] def limit: Long = offset + (1L << (height * 5 + 11))

    @inline private[algebird] def index(n: Int): Int = (n - offset) >>> (height * 5 + 6)
    @inline private[algebird] def valid(i: Int): Boolean = 0 <= i && i < 32
    @inline private[algebird] def invalid(i: Int): Boolean = i < 0 || 32 <= i

    def apply(n: Int): Boolean = {
      val i = index(n)
      valid(i) && {
        val c = children(i)
        c != null && c(n)
      }
    }

    def isEmpty: Boolean = {
      var idx = 0
      while (idx < children.length) {
        val c = children(idx)
        val empty = (c == null) || c.isEmpty
        if (!empty) return false
        idx += 1
      }
      true
    }

    def newChild(i: Int): BitSet = {
      val o = offset + i * (1 << height * 5 + 6)
      if (height == 1) BitSet.newEmpty(o)
      else Branch(o, height - 1, new Array[BitSet](32))
    }

    def +(n: Int): BitSet = {
      val i = index(n)
      if (invalid(i)) {
        BitSet.adoptedPlus(this, n)
      } else {
        val c0 = children(i)
        val c1 =
          if (c0 != null) c0 + n
          else {
            val cc = newChild(i)
            cc += n
            cc
          }
        // we already had this item
        if (c0 eq c1) this
        else replace(i, c1)
      }
    }

    def replace(i: Int, child: BitSet): Branch = {
      val cs = new Array[BitSet](32)
      System.arraycopy(children, 0, cs, 0, 32)
      cs(i) = child
      copy(children = cs)
    }

    def -(n: Int): BitSet = {
      val i = index(n)
      if (invalid(i)) this
      else {
        val c = children(i)
        if (c == null) this
        else {
          val c1 = c - n
          if (c1 eq c) this // we don't contain n
          else replace(i, c - n)
        }
      }
    }

    def |(rhs: BitSet): BitSet =
      if (this eq rhs) {
        this
      } else if (height > rhs.height) {
        if (rhs.offset < offset || limit <= rhs.offset) {
          // this branch doesn't contain rhs
          BitSet.adoptedUnion(this, rhs)
        } else {
          // this branch contains rhs, so find its index
          val i = index(rhs.offset)
          val c0 = children(i)
          val c1 =
            if (c0 != null) c0 | rhs
            else if (height == 1) rhs
            else {
              val cc = newChild(i)
              cc |= rhs
              cc
            }
          replace(i, c1)
        }
      } else if (height < rhs.height) {
        // use commuativity to handle this in previous case
        rhs | this
      } else if (offset != rhs.offset) {
        // same height, but non-overlapping
        BitSet.adoptedUnion(this, rhs)
      } else {
        // height == rhs.height, so we know rhs is a Branch.
        val Branch(_, _, rcs) = rhs
        val cs = new Array[BitSet](32)
        var i = 0
        while (i < 32) {
          val x = children(i)
          val y = rcs(i)
          cs(i) = if (x == null) y else if (y == null) x else x | y
          i += 1
        }
        Branch(offset, height, cs)
      }

    def &(rhs: BitSet): BitSet =
      if (this eq rhs) {
        this
      } else if (height > rhs.height) {
        if (rhs.offset < offset || limit <= rhs.offset) {
          Empty
        } else {
          // this branch contains rhs, so find its index
          val i = index(rhs.offset)
          val c0 = children(i)
          if (c0 != null) c0 & rhs else Empty
        }
      } else if (height < rhs.height) {
        // use commuativity to handle this in previous case
        rhs & this
      } else if (offset != rhs.offset) {
        // same height, but non-overlapping
        Empty
      } else {
        // height == rhs.height, so we know rhs is a Branch.
        val Branch(_, _, rcs) = rhs
        val cs = new Array[BitSet](32)
        var i = 0
        var nonEmpty = false
        while (i < 32) {
          val x = children(i)
          val y = rcs(i)
          if (x != null && y != null) {
            val xy = x & y
            if (!(xy eq Empty)) {
              nonEmpty = true
              cs(i) = xy
            }
          }
          i += 1
        }
        if (nonEmpty) Branch(offset, height, cs)
        else Empty
      }

    def intersects(rhs: BitSet): Boolean =
      if (height > rhs.height) {
        if (rhs.offset < offset || limit <= rhs.offset) {
          false
        } else {
          // this branch contains rhs, so find its index
          val i = index(rhs.offset)
          val c0 = children(i)
          if (c0 != null) c0.intersects(rhs) else false
        }
      } else if (height < rhs.height) {
        // use commuativity to handle this in previous case
        rhs.intersects(this)
      } else if (offset != rhs.offset) {
        // same height, but non-overlapping
        false
      } else {
        // height == rhs.height, so we know rhs is a Branch.
        val Branch(_, _, rcs) = rhs
        var i = 0
        while (i < 32) {
          val x = children(i)
          val y = rcs(i)
          if (x != null && y != null && (x.intersects(y))) return true
          i += 1
        }
        false
      }

    def ^(rhs: BitSet): BitSet =
      if (this eq rhs) {
        // TODO: it is unclear why BitSet.Empty isn't okay here.
        // Tests pass if we do it, but it seems a pretty minor optimization
        // If we need some invariant, we should have a test for it.
        // newEmpty(offset)
        //
        // a motivation to use Empty here is to avoid always returning
        // aligned offsets, which make some of the branches below unreachable
        BitSet.Empty
      } else if (height > rhs.height) {
        if (rhs.offset < offset || limit <= rhs.offset) {
          this | rhs
        } else {
          // this branch contains rhs, so find its index
          val i = index(rhs.offset)
          val c0 = children(i)
          if (c0 != null) {
            val cc = c0 ^ rhs
            if (c0 eq cc) this else replace(i, cc)
          } else {
            var cc = rhs
            while (cc.height < height - 1) cc = BitSet.parentFor(cc)
            replace(i, cc)
          }
        }
      } else if (height < rhs.height) {
        // use commuativity to handle this in previous case
        rhs ^ this
      } else if (offset != rhs.offset) {
        // same height, but non-overlapping
        this | rhs
      } else {
        // height == rhs.height, so we know rhs is a Branch.
        val Branch(_, _, rcs) = rhs
        val cs = new Array[BitSet](32)
        var i = 0
        while (i < 32) {
          val c0 = children(i)
          val c1 = rcs(i)
          cs(i) = if (c1 == null) c0 else if (c0 == null) c1 else c0 ^ c1
          i += 1
        }
        Branch(offset, height, cs)
      }

    def --(rhs: BitSet): BitSet =
      rhs match {
        case _ if this eq rhs =>
          Empty
        case b @ Branch(_, _, _) if height < b.height =>
          if (offset < b.offset || b.limit <= offset) this
          else {
            val c = b.children(b.index(offset))
            if (c == null) this else this -- c
          }
        case b @ Branch(_, _, _) if height == b.height =>
          if (offset != b.offset) {
            this
          } else {
            var newChildren: Array[BitSet] = null
            var i = 0
            while (i < 32) {
              val c0 = children(i)
              val c1 = b.children(i)
              val cc = if (c0 == null || c1 == null) c0 else c0 -- c1
              if (!(c0 eq cc)) {
                if (newChildren == null) {
                  newChildren = new Array[BitSet](32)
                  var j = 0
                  while (j < i) {
                    newChildren(j) = children(j)
                    j += 1
                  }
                }
                newChildren(i) = cc
              } else if (newChildren != null) {
                newChildren(i) = c0
              }
              i += 1
            }
            if (newChildren == null) this
            else Branch(offset, height, newChildren)
          }
        case _ /* height > rhs.height */ =>
          if (rhs.offset < offset || limit <= rhs.offset) {
            this
          } else {
            // this branch contains rhs, so find its index
            val i = index(rhs.offset)
            val c = children(i)
            if (c == null) {
              this
            } else {
              val cc = c -- rhs
              if (c eq cc) this else replace(i, cc)
            }
          }
      }

    private[algebird] def +=(n: Int): Unit = {
      val i = index(n)
      val c0 = children(i)
      if (c0 == null) {
        val c = newChild(i)
        children(i) = c
        c += n
      } else {
        c0 += n
      }
    }

    private[algebird] def mutableAdd(n: Int): BitSet = {
      val i = index(n)
      if (valid(i)) {
        val c0 = children(i)
        if (c0 == null) {
          val c = newChild(i)
          children(i) = c
          c += n
        } else {
          c0 += n
        }
        this
      } else {
        BitSet.adoptedPlus(this, n)
      }
    }

    private[algebird] def |=(rhs: BitSet): Unit =
      if (height > rhs.height) {
        if (rhs.offset < offset || limit <= rhs.offset) {
          throw InternalError("union outside of branch jurisdiction")
        } else {
          // this branch contains rhs, so find its index
          val i = index(rhs.offset)
          val c0 = children(i)
          if (c0 == null) {
            val c1 = newChild(i)
            c1 |= rhs
            children(i) = c1
          } else {
            c0 |= rhs
          }
        }
      } else if (height < rhs.height) {
        throw InternalError("branch too short for union")
      } else if (offset != rhs.offset) {
        throw InternalError("branch misaligned")
      } else {
        // height == rhs.height, so we know rhs is a Branch.
        val Branch(_, _, rcs) = rhs
        var i = 0
        while (i < 32) {
          val x = children(i)
          val y = rcs(i)
          if (x == null) children(i) = y
          else if (y != null) x |= rcs(i)
          i += 1
        }
      }

    // TODO: optimize
    def iterator: Iterator[Int] =
      children.iterator.flatMap {
        case null => Iterator.empty
        case c    => c.iterator
      }

    def reverseIterator: Iterator[Int] =
      children.reverseIterator.flatMap {
        case null => Iterator.empty
        case c    => c.reverseIterator
      }

    def size: Long = {
      var i = 0
      var n = 0L
      while (i < 32) {
        val c = children(i)
        if (c != null) n += c.size
        i += 1
      }
      n
    }
  }

  private case class Leaf(offset: Int, private val values: Array[Long]) extends BitSet {

    @inline private[algebird] def limit: Long = offset + 2048L

    @inline private[algebird] def index(n: Int): Int = (n - offset) >>> 6
    @inline private[algebird] def bit(n: Int): Int = (n - offset) & 63

    def height: Int = 0

    def apply(n: Int): Boolean = {
      val i = index(n)
      (0 <= i && i < 32) && (((values(i) >>> bit(n)) & 1) == 1)
    }

    def arrayCopy: Array[Long] = {
      val vs = new Array[Long](32)
      System.arraycopy(values, 0, vs, 0, 32)
      vs
    }

    def +(n: Int): BitSet = {
      val i = index(n)
      if (0 <= i && i < 32) {
        val mask = 1L << bit(n)
        val vsi = values(i)
        if ((vsi & mask) == 1L) this
        else {
          val vs = arrayCopy
          vs(i) = vsi | mask
          Leaf(offset, vs)
        }
      } else {
        BitSet.adoptedPlus(this, n)
      }
    }

    def -(n: Int): BitSet = {
      val i = index(n)
      if (i < 0 || 32 <= i) {
        this
      } else {
        val mask = 1L << bit(n)
        val vsi = values(i)
        if ((vsi & mask) == 0L) this
        else {
          val vs = arrayCopy
          vs(i) = vsi & (~mask)
          Leaf(offset, vs)
        }
      }
    }

    def isEmpty: Boolean = {
      var idx = 0
      while (idx < values.length) {
        val empty = values(idx) == 0L
        if (!empty) return false
        idx += 1
      }
      true
    }

    def size: Long = {
      var c = 0L
      var i = 0
      while (i < 32) {
        c += bitCount(values(i))
        i += 1
      }
      c
    }

    def |(rhs: BitSet): BitSet =
      rhs match {
        case Leaf(offset, values2) =>
          val vs = new Array[Long](32)
          var i = 0
          while (i < 32) {
            vs(i) = values(i) | values2(i)
            i += 1
          }
          Leaf(offset, vs)
        case _ =>
          // TODO: this is the only branch where
          // we could have overlapping positions.
          // if we could be more careful we could
          // allow some mutations in adoptedUnion
          // since we know we never mutate the
          // overlapping part.
          BitSet.adoptedUnion(this, rhs)
      }

    def &(rhs: BitSet): BitSet =
      rhs match {
        case Leaf(o, values2) =>
          if (this eq rhs) {
            this
          } else if (o != offset) {
            Empty
          } else {
            val vs = new Array[Long](32)
            var i = 0
            while (i < 32) {
              vs(i) = values(i) & values2(i)
              i += 1
            }
            Leaf(offset, vs)
          }
        case Branch(_, _, _) =>
          rhs & this
      }

    def intersects(rhs: BitSet): Boolean =
      rhs match {
        case Leaf(o, values2) =>
          if (o != offset) {
            false
          } else {
            var i = 0
            while (i < 32) {
              if ((values(i) & values2(i)) != 0L) return true
              i += 1
            }
            false
          }
        case Branch(_, _, _) =>
          rhs.intersects(this)
      }

    def ^(rhs: BitSet): BitSet =
      rhs match {
        case Leaf(o, values2) =>
          if (this eq rhs) {
            // TODO: it is unclear why BitSet.Empty isn't okay here.
            // Tests pass if we do it, but it seems a pretty minor optimization
            // If we need some invariant, we should have a test for it.
            // newEmpty(offset)
            //
            // a motivation to use Empty here is to avoid always returning
            // aligned offsets, which make some of the branches below unreachable
            BitSet.Empty
          } else if (o != offset) {
            this | rhs
          } else {
            val vs = new Array[Long](32)
            var i = 0
            while (i < 32) {
              vs(i) = values(i) ^ values2(i)
              i += 1
            }
            Leaf(offset, vs)
          }
        case Branch(_, _, _) =>
          rhs ^ this
      }

    def --(rhs: BitSet): BitSet =
      rhs match {
        case Leaf(o, values2) =>
          if (o != offset) {
            this
          } else {
            val vs = new Array[Long](32)
            var i = 0
            while (i < 32) {
              vs(i) = values(i) & (~values2(i))
              i += 1
            }
            Leaf(offset, vs)
          }
        case b @ Branch(_, _, _) =>
          val j = b.index(offset)
          if (0 <= j && j < 32) {
            val c = b.children(j)
            if (c == null) this else this -- c
          } else {
            this
          }
      }

    private[algebird] def +=(n: Int): Unit = {
      val i = index(n)
      val j = bit(n)
      values(i) |= (1L << j)
    }

    private[algebird] def mutableAdd(n: Int): BitSet = {
      val i = index(n)
      if (0 <= i && i < 32) {
        values(i) |= (1L << bit(n))
        this
      } else {
        BitSet.adoptedPlus(this, n)
      }
    }

    private[algebird] def |=(rhs: BitSet): Unit =
      rhs match {
        case Leaf(`offset`, values2) =>
          var i = 0
          while (i < 32) {
            values(i) |= values2(i)
            i += 1
          }
        case _ =>
          throw InternalError("illegal leaf union")
      }

    def iterator: Iterator[Int] =
      new LeafIterator(offset, values)

    def reverseIterator: Iterator[Int] =
      new LeafReverseIterator(offset, values)
  }

  implicit val orderingForBitSet: Ordering[BitSet] = new Ordering[BitSet] {
    override def compare(x: BitSet, y: BitSet): Int = {
      val itx = x.iterator
      val ity = y.iterator
      while (itx.hasNext && ity.hasNext) {
        val c = Integer.compare(itx.next(), ity.next())
        if (c != 0) return c
      }
      if (itx.hasNext) 1
      else if (ity.hasNext) -1
      else 0
    }
  }

  /**
   * Efficient, low-level iterator for BitSet.Leaf values.
   *
   * As mentioned in `BitSet.iterator`, this method will return values
   * in unsigned order (e.g. Int.MaxValue comes before Int.MinValue).
   */
  private class LeafIterator(offset: Int, values: Array[Long]) extends Iterator[Int] {
    var i: Int = 0
    var x: Long = values(0)
    var n: Int = offset

    @tailrec private def search(): Unit =
      if (x == 0 && i < 31) {
        i += 1
        n = offset + i * 64
        x = values(i)
        search()
      } else ()

    private def advance(): Unit = {
      x = x >>> 1
      n += 1
      search()
    }

    search()

    def hasNext: Boolean = x != 0

    def next(): Int = {
      while (x != 0 && (x & 1) == 0) advance()
      if (x == 0) throw new NoSuchElementException("next on empty iterator")
      val res = n
      advance()
      res
    }
  }

  /**
   * Efficient, low-level reversed iterator for BitSet.Leaf values.
   *
   * This class is very similar to LeafIterator but returns values in
   * the reverse order.
   */
  private class LeafReverseIterator(offset: Int, values: Array[Long]) extends Iterator[Int] {
    var i: Int = 31
    var x: Long = values(31)
    var n: Int = offset + (i + 1) * 64 - 1

    @tailrec private def search(): Unit =
      if (x == 0 && i > 0) {
        i -= 1
        n = offset + (i + 1) * 64 - 1
        x = values(i)
        search()
      } else ()

    private def advance(): Unit = {
      x = x << 1
      n -= 1
      search()
    }

    search()

    def hasNext: Boolean = x != 0

    def next(): Int = {
      while (x > 0) advance()
      if (x == 0) throw new NoSuchElementException("next on empty iterator")
      val res = n
      advance()
      res
    }
  }
}

// scalastyle:on

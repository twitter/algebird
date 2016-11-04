---
layout: docs
title:  "Learning Algebird at the REPL"
section: "data"
---

# Learning Algebird Monoids at the REPL

- [Overview](#overview)
- [Preliminaries](#prelim)
- [Max](#max)
- [Hyperloglog](#hll)
- [Bloom Filter](#bloom)
- [QTree](#qtree)
- [Count Min Sketch](#cms)
- [Decayed Value (aka. moving average)](#decayed_value)
- [Sketch Map](#sketch_map)
- [Min](#min)
- [General - Adding and Multiplication of collections](#general)

## <a id="overview" href="#overview"></a> Overview

The goal of this page is to show you how easy it is to learn and experiment with Algebird data structures with a REPL. Most of the examples are taken from Algebird tests.

It is helpful to have a conceptual understanding of Algebird. Check out the tutorials at [Resources for Learners]({{ site.baseurl }}/resources_for_learners.html).

## <a id="prelim" href="#prelim"></a> Preliminaries

Clone the repository:

```
$ git clone https://github.com/twitter/algebird.git
$ cd algebird
$ ./sbt "project algebird-core" console
```

Import algebird:

```tut
import com.twitter.algebird._
```

## <a id="max" href="#max"></a> Max

Example from <http://www.michael-noll.com/blog/2013/12/02/twitter-algebird-monoid-monad-for-large-scala-data-analytics/>

```tut
import com.twitter.algebird.Operators._
Max(10) + Max(30) + Max(20)

case class TwitterUser(val name: String, val numFollowers: Int) extends Ordered[TwitterUser] {
  def compare(that: TwitterUser): Int = {
    val c = this.numFollowers - that.numFollowers
    if (c == 0) this.name.compareTo(that.name) else c
  }
}
```

Let's have a popularity contest on Twitter.  The user with the most followers wins!

```tut
val barackobama = TwitterUser("BarackObama", 40267391)
val katyperry = TwitterUser("katyperry", 48013573)
val ladygaga = TwitterUser("ladygaga", 40756470)
val miguno = TwitterUser("miguno", 731) // I participate, too.  Olympic spirit!
val taylorswift = TwitterUser("taylorswift13", 37125055)
val winner: Max[TwitterUser] = Max(barackobama) + Max(katyperry) + Max(ladygaga) + Max(miguno) + Max(taylorswift)

assert(winner.get == katyperry)
```

## <a id="hll" href="#hll"></a> Hyperloglog

HyperLogLog is an approximation algorithm that estimates the number of distinct elements in a multiset using a constant amount of memory. (See https://en.wikipedia.org/wiki/HyperLogLog)

In Algebird, the HLL class can be used like a set. You can create an HLL from an element, and you can combine HLLs using the `+` operation, which is like set union. Like most structures in Algebird, HLLs form a monoid, where set union (`+`) is the addition operation.

Unlike a normal set, HLL cannot lookup whether elements are in the set. If you want to do this, consider using a BloomFilter, which can check elements for set membership, but is less memory-efficient than a HLL.

> Note: Internally, HLL represents its elements using hashes, which are internally represented as `Array[Byte]`. For this reason, some HLL methods take an `Array[Byte]`, assuming that the user will convert the object to an `Array[Byte]`. However, to make using the HLLs easier, there are also generic methods that take any type `K` as long as there's evidence of a `Hash128[K]`, which represents a 128-bit hash function for objects of type `K`.

HLLs cannot be instantiated directly. Instead, you must create them using one of the following strategies:

### HyperLogLogMonoid

The `HyperLogLogMonoid` class is the simplest way to create HLLs. `HyperLogLogMonoid` is quite barebones, so, in most situations, it makes sense to use the `HyperLogLogAggregator` methods mentioned below.

#### Step 1: Create a HyperLogLogMonoid

The HyperLogLogMonoid constructor takes an Int `bits`, which represents the number of bits of the hash function that the HLL uses. The more bits you use, the more space the HLLs will take up, and the more precise your estimates will be. For a better understanding of the space-to-accuracy trade-off, see [this table](https://github.com/twitter/algebird/blob/develop/algebird-core/src/main/scala/com/twitter/algebird/HyperLogLog.scala#L197) or use one of the other strategies mentioned below, which allow you to specify the desired error.

```tut
val hllMonoid = new HyperLogLogMonoid(bits = 4)
```

#### Step 2: Create HLLs from your data

HyperLogLogMonoid has a `create` method which takes a hashed element (as a `Array[Byte]`) and returns an `HLL` which represents a set containing the given element.

We can create an HLL containing a list of elements by creating HLLs for each element using the `create` method, and combining the elements using the HyperLogLogMonoid's `sum` method.

```tut
import com.twitter.algebird.HyperLogLog.int2Bytes
val data = List(1, 1, 2, 2, 3, 3, 4, 4, 5, 5)
val hlls = data.map { hllMonoid.create(_) }
val combinedHLL = hllMonoid.sum(hlls)
```

Note that we were able to call `hllMonoid.create` on an `Int` because we imported the implicit conversion `int2Bytes` that converts an `Int` into a `Array[Byte]`.

#### Step 3: Approximating set cardinality using an HLL

We can use the `sizeOf` method to estimate the approximate number of distinct elements in the multiset.

```tut
val approxSizeOf = hllMonoid.sizeOf(combinedHLL)
```

### HyperLogLogAggregator

The `HyperLogLogAggregator` object contains some methods that make it easy to instantiate Algebird Aggregators that internally use HLLs.

We will only mention the generic Aggregators here. The generic aggregators require that we have an implicit `Hash128[K]` instance. Algebird has instances of `Hash128` for many common types such as `Int`, `Long` and `String`.

To learn more about the `Array[Byte]` aggregators, see [the source code of HyperLogLog](https://github.com/twitter/algebird/blob/develop/algebird-core/src/main/scala/com/twitter/algebird/HyperLogLog.scala).

### `HyperLogLogAggregator.withErrorGeneric[K: Hash128](error: Double)`

This is an aggregator of type `Aggregator[K, HLL, HLL]`, which means that it builds a `HLL` from a `TraversableOnce` of `K`s.
It takes an `error`, which must be a Double in the range (0,1).

```tut
val agg = HyperLogLogAggregator.withErrorGeneric[Int](0.01)
val data = List(1, 1, 2, 2, 3, 3, 4, 4, 5, 5)
val combinedHll: HLL = agg(data)
```

### `HyperLogLogAggregator.withBits[K: Hash128](bits: Int)`

Similar to `withErrorGeneric`, but takes the number of bits as an Int.

```tut
val agg = HyperLogLogAggregator.withBits[Int](9)
val data = List(1, 1, 2, 2, 3, 3, 4, 4, 5, 5)
val combinedHll: HLL = agg(data)
```

### `HyperLogLogAggregator.sizeWithErrorGeneric[K: Hash128](err: Double)`

This is an aggregator of type `Aggregator[K, HLL, Long]`, which means that it presents a Long value. The Long that it returns is the estimated size of the combined HLL.

```tut
val agg = HyperLogLogAggregator.sizeWithErrorGeneric[Int](0.01)
val data = List(1, 1, 2, 2, 3, 3, 4, 4, 5, 5)
val approximateSize: Long = agg(data)
```

### REPL Tour

```tut
import HyperLogLog._
val hll = new HyperLogLogMonoid(4)
val data = List(1, 1, 2, 2, 3, 3, 4, 4, 5, 5)
val seqHll = data.map { hll(_) }
val sumHll = hll.sum(seqHll)
val approxSizeOf = hll.sizeOf(sumHll)
val actualSize = data.toSet.size
val estimate = approxSizeOf.estimate
```

## <a id="bloom" href="#bloom"></a> Bloom Filter

```tut
import com.twitter.algebird._
val NUM_HASHES = 6
val WIDTH = 32
val SEED = 1
val bfMonoid = new BloomFilterMonoid(NUM_HASHES, WIDTH, SEED)
val bf = bfMonoid.create("1", "2", "3", "4", "100")
val approxBool = bf.contains("1")
val res = approxBool.isTrue
```

## <a id="qtree" href="#qtree"></a> QTree

A `QTree[A: Monoid]` is like an approximate `Map[Double, A: Monoid]` that can store only a fixed number of keys. Instead of storing key-value pairs, it stores range-value pairs, where each range is a small Double interval, and the ranges don't overlap. When there are too many keys, it combines nearby range-value pairs, combining the ranges, and combining the values using the `Monoid[A]` operation. Each range-value pair also keeps track of its count, i.e. the number of key-value pairs the range is composed of.

For example, suppose we create a QTree[String] from the following values: (remember that `String` is a `Monoid` under concatenation)

- 0.1, "he"
- 0.2, "ll"
- 0.21, "o"
- 0.26, "wo"
- 0.3, "r"
- 0.4, "l"
- 0.49, "d"

This QTree might be represented as

- \[0,0.125), "he", count=1
- \[0.125,0.25), "llo", count=2
- \[0.25, 0.375), "wor", count=2
- \[0.375, 0.5), "ld", count=2

or as

- \[0, 0.25), "hello", count=3
- \[0.25, 0.5), "world", count=4

or as

- \[0,0.5), "helloworld", count=7

How is this useful?

The most common usage of `QTree` is as `QTree[Unit]`. (Remember, `Unit` is a trivial monoid, where `() + () = ()` and `()` is the identity.)

This is basically a histogram. Why? When two ranges get combined, the counts get added together, so at any point, the count associated with a range R is exactly the number of original Double values in that range.
- The `quantileBounds` function gives lower and upper bounds for a given quantile.
  For instance, `qtree.quantileBounds(0.5)` gives bounds for the median double value.
- The `rangeCountBounds` function gives lower and upper bounds for the count within a certain range.
  For instance, `qtree.rangeCountBounds(0.5, 2.5)` gives lower and upper bounds for the number of elements in the range (0.5, 2.5).

What happens if we use a nontrivial monoid `A`, instead of `Unit`? Then we get some more useful functions:
- `rangeSumBounds` gives lower and upper bounds for the monoid sum of the elements in a certain range.

For example, if we call `qtree.rangeSumBounds(0.1,0.2)` on the first QTree described above, it would return `("","hello")`. Why is it a range, and not an exact value? This is because we only store the ranges and we don't know exactly where in the range each item is. For example, `"he"` might exist between 0 and 0.09999 and `"llo"` might exist between 0.20001 and 0.25, in which case the true sum over (0.1,0.2) would be `""`. On the other hand, `"he"` might exist between 0.1 and 0.125, and `"llo"` might exist between 0.125 and 0.2, in which case the true sum would be `"hello"`. The answer could also be anywhere in between these two results. If we look at the original data points, we see that the correct answer is actually `"he"`, since `"he"` is the only value in the range [0.1,0.2).

- `totalSum` returns the total sum over the entire tree. In the example above, `qtree.totalSum` would return `"helloworld"`.

### REPL Tour

```tut
val data = List(1,1,2,2,3,3,4,4,5,5,6,6,7,7,8,8)
val seqQTree = data.map { QTree(_) }
import com.twitter.algebird.QTreeSemigroup
val qtSemigroup = new QTreeSemigroup[Long](6)
val sum = qtSemigroup.sumOption(seqQTree).get
val sum2 = seqQTree.reduce{qtSemigroup.plus(_,_)}
sum.count
sum.upperBound
sum.lowerBound
sum.quantileBounds(.5)
sum.quantileBounds(.5)
sum.quantileBounds(.25)
sum.quantileBounds(.75)
```

## <a id="cms" href="#cms"></a> Count Min Sketch

Count-min sketch is a probablistic data structure that estimates the frequencies of elements in a data stream. Count-min sketches are somewhat similar to Bloom filters; the main distinction is that Bloom filters represent sets, while count-min sketches represent multisets. For more info, see [Wikipedia](https://en.wikipedia.org/wiki/Count%E2%80%93min_sketch).

In Algebird, count-min sketches are represented as the abstract class CMS, along with the CMSMonoid class.

```tut
import com.twitter.algebird.CMSHasherImplicits._
val DELTA = 1E-10
val EPS = 0.001
val SEED = 1
val HEAVY_HITTERS_PCT = 0.01
val CMS_MONOID = TopPctCMS.monoid[Long](EPS, DELTA, SEED, HEAVY_HITTERS_PCT)
val data = List(1L, 1L, 3L, 4L, 5L)
val cms = CMS_MONOID.create(data)
cms.totalCount
cms.frequency(1L).estimate
cms.frequency(2L).estimate
cms.frequency(3L).estimate
```

## <a id="value" href="#value"></a> Decayed Value (aka. moving average)

A DecayedValue can be approximately computed into a moving average. Please see an explanation of different averages here: [The Decaying Average](http://donlehmanjr.com/Science/03%20Decay%20Ave/032.htm).

@johnynek mentioned that:

> a DecayedValue is approximately like a moving average value with window size of the half-life. It is EXACTLY a sample of the Laplace transform of the signal of values. Therefore, if we normalize a decayed value with halflife/ln(2), which is the integral of exp(-t(ln(2))/halflife) from 0 to infinity. We get a moving average of the window size equal to halflife.

See the related issue: https://github.com/twitter/algebird/issues/235

Here is the code example for computing a DecayedValue average:

```tut
val data = {
  val rnd = new scala.util.Random
  (1 to 100).map { _ => rnd.nextInt(1000).toDouble }.toSeq
}

val HalfLife = 10.0
val normalization = HalfLife / math.log(2)

implicit val dvMonoid = DecayedValue.monoidWithEpsilon(1e-3)

data.zipWithIndex.scanLeft(Monoid.zero[DecayedValue]) { (previous, data) =>
  val (value, time) = data
  val decayed = Monoid.plus(previous, DecayedValue.build(value, time, HalfLife))
  println("At %d: decayed=%f".format(time, (decayed.value / normalization)))
  decayed
}
```

Running the above code in comparison with a simple decayed average:

```tut
data.zipWithIndex.scanLeft(0.0) { (previous, data) =>
  val (value, time) = data
  val avg = (value + previous * (HalfLife - 1.0)) / HalfLife
  println("At %d: windowed=%f".format(time, avg))
  avg
}
```

You will see that the averages are pretty close to each other.

### DecayedValue FAQ

#### Is a DecayedValue average better than a moving average?

DecayedValue gives an exponential moving average. A standard windowed moving average of N points requires O(N) memory. An exponential moving average takes O(1) memory. That's the win. Usually one, or a few, exponential moving averages gives you what you need cheaper than the naive approach of keeping 100 or 1000 recent points.

#### Is a DecayedValue average better than a simple decayed average?

A simple decayed average looks like this: `val avg = (value + previousAvg * (HaflLife - 1.0)) / HalfLife`

In a way, a DecayedValue average is a simple decayed average with different scaling factor.

## <a id="map" href="#map"></a> Sketch Map

```tut
val DELTA = 1E-8
val EPS = 0.001
val SEED = 1
val HEAVY_HITTERS_COUNT = 10

implicit def string2Bytes(i : String) = i.toCharArray.map(_.toByte)

val PARAMS = SketchMapParams[String](SEED, EPS, DELTA, HEAVY_HITTERS_COUNT)
val MONOID = SketchMap.monoid[String, Long](PARAMS)
val data = List( ("1", 1L), ("3", 2L), ("4", 1L), ("5", 1L) )
val sm = MONOID.create(data)
sm.totalValue
MONOID.frequency(sm, "1")
MONOID.frequency(sm, "2")
MONOID.frequency(sm, "3")
```

## <a id="min" href="#min"></a> Min

```tut
Min(10) + Min(20) + Min(30)
import com.twitter.algebird.Operators._
Min(10) + Min(20) + Min(30)
```

## <a id="general" href="#general"></a> General - Adding and Multiplication of collections

```tut
val data2 = Map(1 -> 1, 2 -> 1)
val data1 =  Map(1 -> 3, 2 -> 5, 3 -> 7, 5 -> 1)
data1 + data2
import com.twitter.algebird.Operators._
data1 + data2
data1 * data2
Set(1,2,3) + Set(3,4,5)
List(1,2,3) + List(3,4,5)
Map(1 -> 3, 2 -> 4, 3 -> 1) * Map(2 -> 2)
Map(1 -> Set(2,3), 2 -> Set(1)) + Map(2 -> Set(2,3))
```

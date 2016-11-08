---
layout: docs
title:  "HyperLogLog"
section: "data"
source: "algebird-core/src/main/scala/com/twitter/algebird/HyperLogLog.scala"
scaladoc: "#com.twitter.algebird.HyperLogLog"
---

# HyperLogLog

HyperLogLog is an approximation algorithm that estimates the number of distinct elements in a multiset using a constant amount of memory. (See https://en.wikipedia.org/wiki/HyperLogLog)

In Algebird, the HLL class can be used like a set. You can create an HLL from an element, and you can combine HLLs using the `+` operation, which is like set union. Like most structures in Algebird, HLLs form a monoid, where set union (`+`) is the addition operation.

Unlike a normal set, HLL cannot lookup whether elements are in the set. If you want to do this, consider using a BloomFilter, which can check elements for set membership, but is less memory-efficient than a HLL.

> Note: Internally, HLL represents its elements using hashes, which are internally represented as `Array[Byte]`. For this reason, some HLL methods take an `Array[Byte]`, assuming that the user will convert the object to an `Array[Byte]`. However, to make using the HLLs easier, there are also generic methods that take any type `K` as long as there's evidence of a `Hash128[K]`, which represents a 128-bit hash function for objects of type `K`.

HLLs cannot be instantiated directly. Instead, you must create them using one of the following strategies:

### HyperLogLogMonoid

The `HyperLogLogMonoid` class is the simplest way to create HLLs. `HyperLogLogMonoid` is quite barebones, so, in most situations, it makes sense to use the `HyperLogLogAggregator` methods mentioned below.

#### Step 1: Create a HyperLogLogMonoid

The HyperLogLogMonoid constructor takes an Int `bits`, which represents the number of bits of the hash function that the HLL uses. The more bits you use, the more space the HLLs will take up, and the more precise your estimates will be. For a better understanding of the space-to-accuracy trade-off, see [this table](https://github.com/twitter/algebird/blob/develop/algebird-core/src/main/scala/com/twitter/algebird/HyperLogLog.scala#L197) or use one of the other strategies mentioned below, which allow you to specify the desired error.

```tut:book
import com.twitter.algebird._
val hllMonoid = new HyperLogLogMonoid(bits = 4)
```

#### Step 2: Create HLLs from your data

HyperLogLogMonoid has a `create` method which takes a hashed element (as a `Array[Byte]`) and returns an `HLL` which represents a set containing the given element.

We can create an HLL containing a list of elements by creating HLLs for each element using the `create` method, and combining the elements using the HyperLogLogMonoid's `sum` method.

```tut:book
import com.twitter.algebird.HyperLogLog.int2Bytes
val data = List(1, 1, 2, 2, 3, 3, 4, 4, 5, 5)
val hlls = data.map { hllMonoid.create(_) }
val combinedHLL = hllMonoid.sum(hlls)
```

Note that we were able to call `hllMonoid.create` on an `Int` because we imported the implicit conversion `int2Bytes` that converts an `Int` into a `Array[Byte]`.

#### Step 3: Approximating set cardinality using an HLL

We can use the `sizeOf` method to estimate the approximate number of distinct elements in the multiset.

```tut:book
val approxSizeOf = hllMonoid.sizeOf(combinedHLL)
```

### HyperLogLogAggregator

The `HyperLogLogAggregator` object contains some methods that make it easy to instantiate Algebird Aggregators that internally use HLLs.

We will only mention the generic Aggregators here. The generic aggregators require that we have an implicit `Hash128[K]` instance. Algebird has instances of `Hash128` for many common types such as `Int`, `Long` and `String`.

To learn more about the `Array[Byte]` aggregators, see [the source code of HyperLogLog](https://github.com/twitter/algebird/blob/develop/algebird-core/src/main/scala/com/twitter/algebird/HyperLogLog.scala).

### `HyperLogLogAggregator.withErrorGeneric[K: Hash128](error: Double)`

This is an aggregator of type `Aggregator[K, HLL, HLL]`, which means that it builds a `HLL` from a `TraversableOnce` of `K`s.
It takes an `error`, which must be a Double in the range (0,1).

```tut:book
val agg = HyperLogLogAggregator.withErrorGeneric[Int](0.01)
val data = List(1, 1, 2, 2, 3, 3, 4, 4, 5, 5)
val combinedHll: HLL = agg(data)
```

### `HyperLogLogAggregator.withBits[K: Hash128](bits: Int)`

Similar to `withErrorGeneric`, but takes the number of bits as an Int.

```tut:book
val agg = HyperLogLogAggregator.withBits[Int](9)
val data = List(1, 1, 2, 2, 3, 3, 4, 4, 5, 5)
val combinedHll: HLL = agg(data)
```

### `HyperLogLogAggregator.sizeWithErrorGeneric[K: Hash128](err: Double)`

This is an aggregator of type `Aggregator[K, HLL, Long]`, which means that it presents a Long value. The Long that it returns is the estimated size of the combined HLL.

```tut:book
val agg = HyperLogLogAggregator.sizeWithErrorGeneric[Int](0.01)
val data = List(1, 1, 2, 2, 3, 3, 4, 4, 5, 5)
val approximateSize: Long = agg(data)
```

### REPL Tour

```tut:book
import HyperLogLog._
val hll = new HyperLogLogMonoid(4)
val data = List(1, 1, 2, 2, 3, 3, 4, 4, 5, 5)
val seqHll = data.map { hll(_) }
val sumHll = hll.sum(seqHll)
val approxSizeOf = hll.sizeOf(sumHll)
val actualSize = data.toSet.size
val estimate = approxSizeOf.estimate
```

### Documentation Help

We'd love your help fleshing out this documentation! You can edit this page in your browser by clicking [this link](https://github.com/twitter/algebird/edit/develop/docs/src/main/tut/datatypes/approx/hyperloglog.md). These links might be helpful:

- [HyperLogLog.scala](https://github.com/twitter/algebird/blob/develop/algebird-core/src/main/scala/com/twitter/algebird/HyperLogLog.scala)
- [HyperLogLogTest.scala](https://github.com/twitter/algebird/blob/develop/algebird-test/src/test/scala/com/twitter/algebird/HyperLogLogTest.scala)
- [HllBatchCreateBenchmark.scala](https://github.com/twitter/algebird/blob/develop/algebird-benchmark/src/main/scala/com/twitter/algebird/benchmark/HllBatchCreateBenchmark.scala)
- [HLLBenchmark.scala](https://github.com/twitter/algebird/blob/develop/algebird-benchmark/src/main/scala/com/twitter/algebird/benchmark/HLLBenchmark.scala)
- [HLLPresentBenchmark.scala](https://github.com/twitter/algebird/blob/develop/algebird-benchmark/src/main/scala/com/twitter/algebird/benchmark/HLLPresentBenchmark.scala)

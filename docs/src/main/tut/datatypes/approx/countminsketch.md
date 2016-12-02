---
layout: docs
title:  "Count Min Sketch"
section: "data"
source: "algebird-core/src/main/scala/com/twitter/algebird/CountMinSketch.scala"
scaladoc: "#com.twitter.algebird.CountMinSketch"
---

# Count Min Sketch

Count-min sketch is a probablistic data structure that estimates the frequencies of elements in a data stream. Count-min sketches are somewhat similar to Bloom filters; the main distinction is that Bloom filters represent sets, while count-min sketches represent multisets. For more info, see [Wikipedia](https://en.wikipedia.org/wiki/Count%E2%80%93min_sketch).

In Algebird, count-min sketches are represented as the abstract class `CMS`, along with the `CMSMonoid` class. Here's an example usage:

```tut:book
import com.twitter.algebird._
import CMSHasherImplicits._
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

## CMS Hasher

The Count-Min sketch uses `d` (aka `depth`) pair-wise independent hash functions drawn from a universal hashing family of the form:

```scala
h(x) = [a * x + b (mod p)] (mod m)
```

As a requirement for using `CMS` you must provide an implicit `CMSHasher[K]` for the type `K` of the items you want to count.  Algebird ships with several such implicits for commonly used types `K` such as `Long` and `scala.BigInt`.

If your type `K` is not supported out of the box, you have two options:

1. You provide a "translation" function to convert items of your (unsupported) type `K` to a supported type such as `Double`, and then use the `contramap` function of `CMSHasher` to create the required `CMSHasher[K]` for your type (see the documentation of `contramap` for an example)
2. You implement a `CMSHasher[K]` from scratch, using the existing `CMSHasher` implementations as a starting point.

# Sketch Map

A Sketch Map is a generalized version of the Count-Min Sketch that is an approximation of Map[K, V] that stores reference to top heavy hitters. The Sketch Map can approximate the sums of any summable value that has a monoid.

```tut:book
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

### Documentation Help

We'd love your help fleshing out this documentation! You can edit this page in your browser by clicking [this link](https://github.com/twitter/algebird/edit/develop/docs/src/main/tut/datatypes/approx/countminsketch.md). These links might be helpful:

- [CountMinSketch.scala](https://github.com/twitter/algebird/blob/develop/algebird-core/src/main/scala/com/twitter/algebird/CountMinSketch.scala)
- [CountMinSketchTest.scala](https://github.com/twitter/algebird/blob/develop/algebird-test/src/test/scala/com/twitter/algebird/CountMinSketchTest.scala)
- [CMSHasher.scala](https://github.com/twitter/algebird/blob/develop/algebird-core/src/main/scala/com/twitter/algebird/CMSHasher.scala)
- [SketchMap.scala](https://github.com/twitter/algebird/blob/develop/algebird-core/src/main/scala/com/twitter/algebird/SketchMap.scala)
- [SketchMapTest.scala](https://github.com/twitter/algebird/blob/develop/algebird-test/src/test/scala/com/twitter/algebird/SketchMapTest.scala)
- [CMSHashingBenchmark.scala](https://github.com/twitter/algebird/blob/develop/algebird-benchmark/src/main/scala/com/twitter/algebird/benchmark/CMSHashingBenchmark.scala)
- [TopCMSBenchmark.scala](https://github.com/twitter/algebird/blob/develop/algebird-benchmark/src/main/scala/com/twitter/algebird/benchmark/TopCMSBenchmark.scala)

---
layout: docs
title:  "Q Tree"
section: "data"
source: "algebird-core/src/main/scala/com/twitter/algebird/QTree.scala"
scaladoc: "#com.twitter.algebird.QTree"
---

# Q Tree

A `QTree[A: Monoid]` is like an approximate `Map[Double, A: Monoid]` that can store only a fixed number of keys. Instead of storing key-value pairs, it stores range-value pairs, where each range is a small `Double` interval, and the ranges don't overlap. When there are too many keys, it combines nearby range-value pairs, combining the ranges, and combining the values using the `Monoid[A]` operation. Each range-value pair also keeps track of its count, i.e. the number of key-value pairs the range is composed of.

For example, suppose we create a `QTree[String]` from the following values: (remember that `String` is a `Monoid` under concatenation)

```
- 0.1, "he"
- 0.2, "ll"
- 0.21, "o"
- 0.26, "wo"
- 0.3, "r"
- 0.4, "l"
- 0.49, "d"
```

This QTree might be represented as

```
- \[0,0.125), "he", count=1
- \[0.125,0.25), "llo", count=2
- \[0.25, 0.375), "wor", count=2
- \[0.375, 0.5), "ld", count=2
```

or as

```
- \[0, 0.25), "hello", count=3
- \[0.25, 0.5), "world", count=4
```

or as

```
- \[0,0.5), "helloworld", count=7
```

How is this useful?

The most common usage of `QTree` is as `QTree[Unit]`. (Remember, `Unit` is a trivial monoid, where `() + () = ()` and `()` is the identity.)

This is basically a histogram. Why? When two ranges get combined, the counts get added together, so at any point, the count associated with a range `R` is exactly the number of original `Double` values in that range.

- The `quantileBounds` function gives lower and upper bounds for a given quantile.
  For instance, `qtree.quantileBounds(0.5)` gives bounds for the median double value.
- The `rangeCountBounds` function gives lower and upper bounds for the count within a certain range.
  For instance, `qtree.rangeCountBounds(0.5, 2.5)` gives lower and upper bounds for the number of elements in the range (0.5, 2.5).

What happens if we use a nontrivial monoid `A`, instead of `Unit`? Then we get some more useful functions:

- `rangeSumBounds` gives lower and upper bounds for the monoid sum of the elements in a certain range.

For example, if we call `qtree.rangeSumBounds(0.1,0.2)` on the first `QTree` described above, it would return `("","hello")`. Why is it a range, and not an exact value? This is because we only store the ranges and we don't know exactly where in the range each item is. For example, `"he"` might exist between 0 and 0.09999 and `"llo"` might exist between 0.20001 and 0.25, in which case the true sum over (0.1,0.2) would be `""`. On the other hand, `"he"` might exist between 0.1 and 0.125, and `"llo"` might exist between 0.125 and 0.2, in which case the true sum would be `"hello"`. The answer could also be anywhere in between these two results. If we look at the original data points, we see that the correct answer is actually `"he"`, since `"he"` is the only value in the range [0.1,0.2).

- `totalSum` returns the total sum over the entire tree. In the example above, `qtree.totalSum` would return `"helloworld"`.

## REPL Tour

```scala mdoc
import com.twitter.algebird._
val data = List(1,1,2,2,3,3,4,4,5,5,6,6,7,7,8,8)
val seqQTree = data.map { QTree(_) }
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

### Documentation Help

We'd love your help fleshing out this documentation! You can edit this page in your browser by clicking [this link](https://github.com/twitter/algebird/edit/develop/docs/src/main/tut/datatypes/approx/q_tree.md). These links might be helpful:

- [QTree.scala](https://github.com/twitter/algebird/blob/develop/algebird-core/src/main/scala/com/twitter/algebird/QTree.scala)
- [QTreeTest.scala](https://github.com/twitter/algebird/blob/develop/algebird-test/src/test/scala/com/twitter/algebird/QTreeTest.scala)
- [QTreeBenchmark.scala](https://github.com/twitter/algebird/blob/develop/algebird-benchmark/src/main/scala/com/twitter/algebird/benchmark/QTreeBenchmark.scala)
- [QTreeMicroBenchmark.scala](https://github.com/twitter/algebird/blob/develop/algebird-benchmark/src/main/scala/com/twitter/algebird/benchmark/QTreeMicroBenchmark.scala)

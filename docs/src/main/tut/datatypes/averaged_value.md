---
layout: docs
title:  "Averaged Value"
section: "data"
source: "algebird-core/src/main/scala/com/twitter/algebird/AveragedValue.scala"
scaladoc: "#com.twitter.algebird.AveragedValue"
---

# Averaged Value

The `AveragedValue` data structure keeps track of the `count` and `mean` of a stream of numbers with a single pass over the data. The mean calculation uses a numerically stable online algorithm suitable for large numbers of records, similar to Chan et. al.'s [parallel variance algorithm on Wikipedia](http://en.wikipedia.org/wiki/Algorithms_for_calculating_variance#Parallel_algorithm). As long as your count doesn't overflow a `Long`, the mean calculation won't overflow.

You can build instances of `AveragedValue` from any numeric type:

```tut:book
import com.twitter.algebird._

val longVal = AveragedValue(3L)
val doubleVal = AveragedValue(12.0)
val intVal = AveragedValue(15)
```

## Algebraic Properties

Combining instances with `+` generates a new instance by adding the `count`s and averaging the `value`s:

```tut:book
longVal + doubleVal
longVal + doubleVal + intVal
```

You can also add numbers directly to an `AveragedValue` instance:

```tut:book
longVal + 12
```

`AveragedValue` is a commutative group. This means you can add instances in any order:

```tut:book
longVal + doubleVal == doubleVal + doubleVal
```

An `AveragedValue` with a count and value of `0` act as `Monoid.zero`:

```tut:book
Monoid.zero[AveragedValue]
longVal + Monoid.zero[AveragedValue] == longVal
```

Subtracting `AveragedValue`s is the opposite of addition:

```tut:book
intVal - longVal
intVal + doubleVal - doubleVal
```

### Stable Average Algorithm

Each `AveragedValue` instance represents a stream of data. `AveragedValue` uses Chan et. al.'s [parallel algorithm](http://en.wikipedia.org/wiki/Algorithms_for_calculating_variance#Parallel_algorithm) to combine the mean values of each stream. Here's the calculation:

```scala
// big and small are two AveragedValue instances
val deltaOfMeans = big.value - small.value
val newCount = big.count + small.count
val newMean = small.value + deltaOfMeans * (big.count / newCount)
```

As long as `newCount` stays within the bounds of `Long`, this calculation won't overflow for large `value`.

The [Wikipedia presentation of this algorithm](http://en.wikipedia.org/wiki/Algorithms_for_calculating_variance#Parallel_algorithm) points out that if both streams are close in size, the algorithm is "prone to loss of precision due to [catastrophic cancellation](https://en.wikipedia.org/wiki/Loss_of_significance)".

`AveragedValue` guards against this instability by taking a weighted average of the two instances if the smaller of the two has a count greater than `10%` of the combined count.

```scala
val newCount = big.count + small.count
(big.count * big.value + small.count * small.value) / newCount
```

## Aggregator

`AveragedValue.aggregator` returns an `Aggregator` that uses `AveragedValue` to calculate the mean of all `Double` values in a stream. For example:

```tut:book
val items = List[Double](1.0, 2.2, 3.3, 4.4, 5.5)
AveragedValue.aggregator(items)
```

`AveragedValue.numericAggregator` works the same way for any numeric type:

```tut:book
val items = List[Int](1, 3, 5, 7)
AveragedValue.numericAggregator[Int].apply(items)
```

## Related Data Structures

- [DecayedValue](decayed_value.html) calculates a decayed moving average of a stream; this allows you to approximate the mean of a bounded sliding window of the datastream.
- In addition to count and mean, [Moments](five_moments.html) allows you to keep track of the standard deviation, skewness and kurtosis of a stream with one pass over the data.

### Links

- Source: [AveragedValue.scala](https://github.com/twitter/algebird/blob/develop/algebird-core/src/main/scala/com/twitter/algebird/AveragedValue.scala)
- Tests: [AveragedValueLaws.scala](https://github.com/twitter/algebird/blob/develop/algebird-test/src/test/scala/com/twitter/algebird/AveragedValueLaws.scala)
- Scaladoc: [AveragedValue]({{site.baseurl}}/api#com.twitter.algebird.AveragedValue)

### Documentation Help

We'd love your help fleshing out this documentation! You can edit this page in your browser by clicking [this link](https://github.com/twitter/algebird/edit/develop/docs/src/main/tut/datatypes/averaged_value.md).

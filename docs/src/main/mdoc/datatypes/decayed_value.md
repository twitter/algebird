---
layout: docs
title:  "Decayed Value"
section: "data"
source: "algebird-core/src/main/scala/com/twitter/algebird/DecayedValue.scala"
scaladoc: "#com.twitter.algebird.DecayedValue"
---

# Decayed Value (aka. moving average)

A `DecayedValue` can be approximately computed into a moving average. Please see an explanation of different averages here: [The Decaying Average](http://donlehmanjr.com/Science/03%20Decay%20Ave/032.htm).

[@johnynek](https://www.github.com/johnynek) mentioned that:

> a DecayedValue is approximately like a moving average value with window size of the half-life. It is EXACTLY a sample of the Laplace transform of the signal of values. Therefore, if we normalize a decayed value with halflife/ln(2), which is the integral of exp(-t(ln(2))/halflife) from 0 to infinity. We get a moving average of the window size equal to halflife.

See the related issue: https://github.com/twitter/algebird/issues/235

Here is the code example for computing a `DecayedValue` average:

```scala mdoc
import com.twitter.algebird._

val data = {
  val rnd = new scala.util.Random
  (1 to 10).map { _ => rnd.nextInt(1000).toDouble }.toSeq
}

val HalfLife = 5.0
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

```scala mdoc
data.zipWithIndex.scanLeft(0.0) { (previous, data) =>
  val (value, time) = data
  val avg = (value + previous * (HalfLife - 1.0)) / HalfLife
  println("At %d: windowed=%f".format(time, avg))
  avg
}
```

You'll see that the averages are pretty close to each other.

## DecayedValue FAQ

### Is a DecayedValue average better than a moving average?

DecayedValue gives an exponential moving average. A standard windowed moving average of `N` points requires `O(N)` memory. An exponential moving average takes `O(1)` memory. That's the win. Usually one, or a few, exponential moving averages gives you what you need cheaper than the naive approach of keeping 100 or 1000 recent points.

### Is a DecayedValue average better than a simple decayed average?

A simple decayed average looks like this:

```scala
val avg = (value + previousAvg * (HaflLife - 1.0)) / HalfLife
```

In a way, a `DecayedValue` average is a simple decayed average with different scaling factor.

### Documentation Help

We'd love your help fleshing out this documentation! You can edit this page in your browser by clicking [this link](https://github.com/twitter/algebird/edit/develop/docs/src/main/tut/datatypes/decayed_value.md). These links might be helpful:

- [DecayedValue.scala](https://github.com/twitter/algebird/blob/develop/algebird-core/src/main/scala/com/twitter/algebird/DecayedValue.scala)
- [DecayedValueTest.scala](https://github.com/twitter/algebird/blob/develop/algebird-test/src/test/scala/com/twitter/algebird/DecayedValueTest.scala)

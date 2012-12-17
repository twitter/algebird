## Algebird [![Build Status](https://secure.travis-ci.org/twitter/algebird.png)](http://travis-ci.org/twitter/algebird)

Abstract algebra for Scala. This code is targeted at building aggregation systems (via [Scalding](https://github.com/twitter/scalding) or [Storm](https://github.com/nathanmarz/storm)). It was originally developed as part of Scalding's Matrix API, where Matrices had values which are elements of Monoids, Groups, or Rings. Subsequently, it was clear that the code had broader application within Scalding and on other projects within Twitter.

## What can you do with this code?

```scala
Welcome to Scala version 2.9.2 (Java HotSpot(TM) 64-Bit Server VM, Java 1.7.0_07).
Type in expressions to have them evaluated.
Type :help for more information.

scala> import com.twitter.algebird._
import com.twitter.algebird._

scala> import com.twitter.algebird.Operators._
import com.twitter.algebird.Operators._

scala> Map(1 -> Max(2)) + Map(1 -> Max(3)) + Map(2 -> Max(4))
res1: scala.collection.immutable.Map[Int,com.twitter.algebird.Max[Int]] = Map(2 -> Max(4), 1 -> Max(3))
```
In the above, the class Max[T] signifies that the + operator should actually be max (this is
accomplished by providing an implicit instance of a typeclass for Max that handles +).

* Model a wide class of "reductions" as a sum on some iterator of a particular value type.
For example, average, moving average, max/min, set
  union, approximate set size (in much less memory with HyperLogLog), approximate item counting
  (using CountMinSketch).
* All of these combine naturally in tuples, vectors, maps, options and more standard scala classes.
* Implementations of Monoids for interesting approximation algorithms, such as Bloom filter,
  HyperLogLog and CountMinSketch. These allow you to think of these sophisticated operations like
  you might numbers, and add them up in hadoop or online to produce powerful statistics and
  analytics.

## Maven
Current version is 0.1.5. groupid="com.twitter" artifact="algebird_2.9.2".

## Questions
> Why not use spire?
We didn't know about it when we started this code, but it seems like we're more focused on
large scale analytics.

> Why not use Scalaz's [Monoid](https://github.com/scalaz/scalaz/blob/master/core/src/main/scala/scalaz/Monoid.scala) trait?

The answer is a mix of the following:
* The trait itself is tiny, we just need zero and plus, it is the implementations for all the types that are important. We wrote a code generator to derive instances for all the tuples, and by hand wrote monoids for List, Set, Option, Map, and several other objects used for counting (DecayedValue for exponential decay, AveragedValue for averaging, HyperLogLog for approximate cardinality counting). It's the instances that are useful in scalding and elsewhere.
* We needed this to work in scala 2.8, and it appeared that Scalaz 7 didn't support 2.8. We've since moved to 2.9, though.
* We also needed Ring and Field, and those are not (as of the writing of the code) in Scalaz.
* If you want to interop, it is trivial to define implicit conversions to and from Scalaz Monoid.
* Scalaz is big and scary for some people. We have enough trouble with adoption without scaring people with [Cokleisli star operators](https://github.com/scalaz/scalaz/blob/master/core/src/main/scala/scalaz/Cokleisli.scala#L19).

## Authors

* Oscar Boykin <http://twitter.com/posco>
* Avi Bryant <http://twitter.com/avibryant>
* Edwin Chen <http://twitter.com/echen>
* ellchow <http://github.com/ellchow>
* Mike Gagnon <https://twitter.com/MichaelNGagnon>
* Moses Nakamura <https://twitter.com/mnnakamura>
* Steven Nobel <http://twitter.com/snoble>
* Sam Ritchie <http://twitter.com/sritchie>
* Ashutosh Singhal <http://twitter.com/daashu>
* Argyris Zymnis <http://twitter.com/argyris>

## License
Copyright 2012 Twitter, Inc.

Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0

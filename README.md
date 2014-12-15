## Algebird [![Build Status](https://secure.travis-ci.org/twitter/algebird.png)](http://travis-ci.org/twitter/algebird)

Abstract algebra for Scala. This code is targeted at building aggregation systems (via [Scalding](https://github.com/twitter/scalding) or [Storm](https://github.com/nathanmarz/storm)). It was originally developed as part of Scalding's Matrix API, where Matrices had values which are elements of Monoids, Groups, or Rings. Subsequently, it was clear that the code had broader application within Scalding and on other projects within Twitter.

See the [current API documentation](http://twitter.github.com/algebird) for more information.

## What can you do with this code?

```scala
> ./sbt algebird-core/console

Welcome to Scala version 2.9.3 (Java HotSpot(TM) 64-Bit Server VM, Java 1.7.0_07).
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

## Community and Documentation

This, and all [github.com/twitter](https://github.com/twitter) projects, are under the [Twitter Open Source Code of Conduct](https://engineering.twitter.com/opensource/code-of-conduct). Additionally, see the [Typelevel Code of Conduct](http://typelevel.org/conduct) for specific examples of harassing behavior that are not tolerated.

To learn more and find links to tutorials and information around the web, check out the [Algebird Wiki](https://github.com/twitter/algebird/wiki).

The latest ScalaDocs are hosted on Algebird's [Github Project Page](http://twitter.github.io/algebird).

Discussion occurs primarily on the [Algebird mailing list](https://groups.google.com/forum/#!forum/algebird). Issues should be reported on the [GitHub issue tracker](https://github.com/twitter/algebird/issues).

## Maven

Algebird modules are available on maven central. The current groupid and version for all modules is, respectively, `"com.twitter"` and  `0.8.1`.

Current published artifacts are

* `algebird-core_2.9.3`
* `algebird-core_2.10`
* `algebird-test_2.9.3`
* `algebird-test_2.10`
* `algebird-util_2.9.3`
* `algebird-util_2.10`
* `algebird-bijection_2.9.3`
* `algebird-bijection_2.10`

The suffix denotes the scala version.

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

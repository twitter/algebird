## Algebird

[![Build status](https://img.shields.io/travis/twitter/algebird/develop.svg)](http://travis-ci.org/twitter/algebird)
[![Codecov branch](https://img.shields.io/codecov/c/github/twitter/algebird/develop.svg?maxAge=3600)](https://codecov.io/github/twitter/algebird)
[![Latest version](https://index.scala-lang.org/twitter/algebird/algebird-core/latest.svg?color=orange)](https://index.scala-lang.org/twitter/algebird/algebird-core)
[![Chat](https://badges.gitter.im/twitter/algebird.svg)](https://gitter.im/twitter/algebird?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)

### Overview

Abstract algebra for Scala. This code is targeted at building aggregation systems (via [Scalding](https://github.com/twitter/scalding) or [Apache Storm](http://storm.apache.org/)). It was originally developed as part of Scalding's Matrix API, where Matrices had values which are elements of Monoids, Groups, or Rings. Subsequently, it was clear that the code had broader application within Scalding and on other projects within Twitter.

See the [Algebird website](https://twitter.github.io/algebird) for more information.

### What can you do with this code?

```scala
> ./sbt algebird-core/console

Welcome to Scala version 2.10.5 (Java HotSpot(TM) 64-Bit Server VM, Java 1.7.0_40).
Type in expressions to have them evaluated.
Type :help for more information.

scala> import com.twitter.algebird._
import com.twitter.algebird._

scala> import com.twitter.algebird.Operators._
import com.twitter.algebird.Operators._

scala> Map(1 -> Max(2)) + Map(1 -> Max(3)) + Map(2 -> Max(4))
res0: scala.collection.immutable.Map[Int,com.twitter.algebird.Max[Int]] = Map(2 -> Max(4), 1 -> Max(3))
```

In the above, the class `Max[T]` signifies that the `+` operator should actually be `max` (this is accomplished by providing an implicit instance of a typeclass for `Max` that handles `+`).

- Model a wide class of "reductions" as a sum on some iterator of a particular value type. For example, average, moving average, max/min, set union, approximate set size (in much less memory with HyperLogLog), approximate item counting (using CountMinSketch).
- All of these combine naturally in tuples, vectors, maps, options and more standard scala classes.
- Implementations of Monoids for interesting approximation algorithms, such as Bloom filter, HyperLogLog and CountMinSketch. These allow you to think of these sophisticated operations like you might numbers, and add them up in hadoop or online to produce powerful statistics and analytics.

## Documentation

To learn more and find links to tutorials and information around the web, check out the [Algebird website](https://twitter.github.io/algebird).

The latest API docs are hosted on Algebird's [ScalaDoc index](https://twitter.github.io/algebird/api/).

## Get Involved + Code of Conduct

Pull requests and bug reports are always welcome! Check out our [Contributing guide](https://twitter.github.io/algebird/contributing.html) for information on what we most need help with and how you can get started contributing.

Discussion occurs primarily on the Algebird Gitter channel: [![Chat](https://badges.gitter.im/twitter/algebird.svg)](https://gitter.im/twitter/algebird?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)

We also monitor the [Algebird mailing list](https://groups.google.com/forum/#!forum/algebird).

Issues should be reported on the [GitHub issue tracker](https://github.com/twitter/algebird/issues).

We use a lightweight form of project governance inspired by the one used by Apache projects.

Please see [Contributing and Committership](https://github.com/twitter/analytics-infra-governance#contributing-and-committership) for our code of conduct and our pull request review process.

The TL;DR is send us a pull request, iterate on the feedback + discussion, and get a +1 from a [Committer](COMMITTERS.md) in order to get your PR accepted.

The current list of active committers (who can +1 a pull request) can be found here: [Committers](COMMITTERS.md)

A list of contributors to the project can be found here: [Contributors](https://github.com/twitter/algebird/graphs/contributors)

## Maven

Algebird modules are available on maven central. The current groupid and version for all modules is, respectively, `"com.twitter"` and  `0.13.0`.

See [Algebird's page on the Scaladex](https://index.scala-lang.org/twitter/algebird) for information on all published artifacts and their associated Scala versions. Algebird currently supports Scala 2.10, 2.11 and 2.12.

## Projects using Algebird

- [Scalding](http://github.com/twitter/scalding)
- [Spark](https://github.com/mesos/spark/pull/480)
- [Simmer](https://github.com/avibryant/simmer)
- [Summingbird](https://github.com/twitter/summingbird)
- [Packetloop](https://www.packetloop.com) (see [this tweet](https://twitter.com/cloudjunky/status/355073917720858626)
- Ebay uses Algebird for machine learning: [ScalaDays talk](http://www.slideshare.net/VitalyGordon/scalable-and-flexible-machine-learning-with-scala-linkedin)

Other projects built with Algebird, as compiled by the Scaladex: [![Scaladex Dependents](https://index.scala-lang.org/count.svg?q=dependencies:twitter/algebird*&subject=scaladex:&color=blue&style=flat-square)](https://index.scala-lang.org/search?q=dependencies:twitter/algebird-core)

## Authors

* Oscar Boykin <http://twitter.com/posco>
* Avi Bryant <http://twitter.com/avibryant>
* Edwin Chen <http://twitter.com/echen>
* ellchow <http://github.com/ellchow>
* Mike Gagnon <https://twitter.com/gmike>
* Moses Nakamura <https://twitter.com/mnnakamura>
* Steven Noble <http://twitter.com/snoble>
* Sam Ritchie <http://twitter.com/sritchie>
* Ashutosh Singhal <http://twitter.com/daashu>
* Argyris Zymnis <http://twitter.com/argyris>

## License

Copyright 2016 Twitter, Inc.

Licensed under the [Apache License, Version 2.0](http://www.apache.org/licenses/LICENSE-2.0).

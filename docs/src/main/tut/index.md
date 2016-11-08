---
layout: home
title:  "Home"
section: "home"
---

Algebird is a library which provides abstractions for abstract algebra in the [Scala programming language](https://scala-lang.org).

This code is targeted at building aggregation systems (via [Scalding](https://github.com/twitter/scalding), [Apache Storm](http://storm.apache.org/) or [Summingbird](https://github.com/twitter/summingbird)). It was originally developed as part of Scalding's Matrix API, where Matrices had values which are elements of Monoids, Groups, or Rings. Subsequently, it was clear that the code had broader application within Scalding and on other projects within Twitter.

### What can you do with this code?

```tut:book
import com.twitter.algebird._
import com.twitter.algebird.Operators._
Map(1 -> Max(2)) + Map(1 -> Max(3)) + Map(2 -> Max(4))
```

In the above, the class `Max[T]` signifies that the `+` operator should actually be `max` (this is accomplished by providing an implicit instance of a typeclass for `Max` that handles `+`).

- Model a wide class of "reductions" as a sum on some iterator of a particular value type. For example, average, moving average, max/min, set union, approximate set size (in much less memory with HyperLogLog), approximate item counting (using CountMinSketch).
- All of these combine naturally in tuples, vectors, maps, options and more standard scala classes.
- Implementations of Monoids for interesting approximation algorithms, such as Bloom filter, HyperLogLog and CountMinSketch. These allow you to think of these sophisticated operations like you might numbers, and add them up in hadoop or online to produce powerful statistics and analytics.

## Documentation

The latest API docs are hosted at Algebird's [ScalaDoc index](api/).

## Get Involved + Code of Conduct

Pull requests and bug reports are always welcome!

Discussion occurs primarily on the [Algebird mailing list](https://groups.google.com/forum/#!forum/algebird).
Issues should be reported on the [GitHub issue tracker](https://github.com/twitter/algebird/issues).

We use a lightweight form of project governance inspired by the one used by Apache projects.

Please see [Contributing and Committership](https://github.com/twitter/analytics-infra-governance#contributing-and-committership) for our code of conduct and our pull request review process.

The TL;DR is send us a pull request, iterate on the feedback + discussion, and get a +1 from a [Committer](https://github.com/twitter/algebird/blob/develop/COMMITTERS.md) in order to get your PR accepted.

The current list of active committers (who can +1 a pull request) can be found here: [Committers](https://github.com/twitter/algebird/blob/develop/COMMITTERS.md)

A list of contributors to the project can be found here: [Contributors](https://github.com/twitter/algebird/graphs/contributors)

## License

Copyright 2016 Twitter, Inc.

Licensed under the [Apache License, Version 2.0](http://www.apache.org/licenses/LICENSE-2.0).

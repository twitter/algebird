## Algebird [![Build Status](https://secure.travis-ci.org/twitter/algebird.png)](http://travis-ci.org/twitter/algebird)

Abstract algebra for Scala. This code is targeted at building aggregation systems (via [Scalding](https://github.com/twitter/scalding) or [Storm](https://github.com/nathanmarz/storm)). It was originally developed as part of Scalding's Matrix API, where Matrices had values which are elements of Monoids, Groups, or Rings. Subsequently, it was clear that the code had broader application within Scalding and on other projects within Twitter.

## Maven
Current version is 0.1.1. groupid="com.twitter" artifact="algebird_2.9.2".

## Questions
> Why not use Scalaz's [Monoid](https://github.com/scalaz/scalaz/blob/master/core/src/main/scala/scalaz/Monoid.scala) trait?

The answer is a mix of the following:
* The trait itself is tiny, we just need zero and plus, it is the implementations for all the types that are important. We wrote a code generator to derive instances for all the tuples, and by hand wrote monoids for List, Set, Option, Map, and several other objects used for counting (DecayedValue for exponential decay, AveragedValue for averaging, HyperLogLog for approximate cardinality counting). It's the instances that are useful in scalding and elsewhere.
* We needed this to work in scala 2.8, and it appeared that Scalaz 7 didn't support 2.8. We've since moved to 2.9, though.
* We also needed Ring and Field, and those are not (as of the writing of the code) in Scalaz.
* Scalaz is big and scary for some people. We have enough trouble with adoption without scaring people with [Cokleisli star operators](https://github.com/scalaz/scalaz/blob/master/core/src/main/scala/scalaz/Cokleisli.scala#L19).

## Authors

* Oscar Boykin <http://twitter.com/posco>
* Sam Ritchie <http://twitter.com/sritchie>

## License
Copyright 2012 Twitter, Inc.

Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0

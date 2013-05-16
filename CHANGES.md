# Algebird #

### Version 0.1.13 ###
* Adds QTree monoid for range queries

### Version 0.1.12 ###
* Adds hashing trick monoid for dimensionality reduction

### Version 0.1.10 ###
* Fix compiler flag issue

### Version 0.1.9 ###

* Build `algebird` against scala 2.9.2 and 2.10.0
* `algebird-util` with Future and Try algebras.

### Version 0.1.8 ###

* Break out `algebird-core` and `algebird-test` into separate jars

### Version 0.1.7 ###

* SummingQueue works with capacity of 0, just passes through
* adds compressed bitset (RichCBitSet)
* Add `BFSparse`
* Heavy hitters in countminsketch
* Monad typeclass

### Version 0.1.6 ###

* Adds publishing pom.
* Add SummingQueue
* rename Cassandra's MurmurHash.

### Version 0.1.5 ###
* Make Metric serializable
* JMap uses Semigroup

### Version 0.1.4 ###
* Count-min-sketch (with Monoid)
* Added Bloom Filter (with Monoid)
* HyperLogLog now uses Murmur128 (should be faster)
* Max/Min/First/Last Monoids
* VectorSpace trait (implementations for Maps/Vector)
* DecayedVector for efficient exponential moving average on vectors
* Metric trait
* Approximate[Numeric]/Boolean to track error in approximations
* Adds Semigroup and implicits for usual primitives and collections
* Fixes EitherMonoid to have a zero
* Add MinPlus algebra for shortest path calculations
* Lots of code cleanups

### Version 0.1.2 ###
* Improves speed of HyperLogLog.
* Refactoring of RightFolded Monoid

### Version 0.1.1 ###
* Added Moments monoid for first 5 moments
* Improved HyperLogLogMonoid (less memory usage, added intersections)

### Version 0.1.0 ###
* Moved code over from Scalding.

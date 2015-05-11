# Algebird #

### Version 0.10.0 ###
* EventuallyAggregator and variants #407
* Add MultiAggregator.apply #408
* Return a MonoidAggregator from MultiAggregator when possible #409
* Add SummingWithHitsCache class to also track key hits. #410
* Add MapAggregator to compose tuples of (key, agg) pairs #411
* fix README.md. 2.9.3 no longer published #412
* Add Coveralls Badge to the README #413
* Add some combinators on MonoidAggregator #417
* Added function to safely downsize a HyperLogLog sketch #418
* AdaptiveCache #419
* fix property tests #421
* Make Preparer extend Serializable #422
* Make MutableBackedMap Serializable. #424
* A couple of performance optimizations: HyperLogLog and BloomFilter #426
* Adds a presenting benchmark and optimizes it #427
* Fixed broken links in README #428
* Speed up QTree #433
* Moments returns NaN when count is too low for higher order statistics #434
* Add HLL method to do error-based Aggregator #438
* Bump bijection to 0.8.0 #441

### Version 0.9.0 ###
* Replace mapValues with one single map to avoid serialization in frameworks like Spark. #344
* Add Fold trait for composable incremental processing (for develop) #350
* Add a GC friendly LRU cache to improve SummingCache #341
* BloomFilter should warn or raise on unrealistic input. #355
* GH-345: Parameterize CMS to CMS[K] and decouple counting/querying from heavy hitters #354
* Add Array Monoid & Group. #356
* Improvements to Aggregator #359
* Improve require setup for depth/delta and associated test spec #361
* Bump from 2.11.2 to 2.11.4 #365
* Move to sbt 0.13.5 #364
* Correct wrong comment in estimation function #372
* Add increments to all Summers #373
* removed duplicate semigroup #375
* GH-381: Fix serialization errors when using new CMS implementation in Storm #382
* Fix snoble's name #384
* Lift methods for Aggregator and MonoidAggregator #380
* applyCumulative method on Aggregator #386
* Add Aggregator.zip #389
* GH-388: Fix CMS test issue caused by roundtripping depth->delta->depth #395
* GH-392: Improve hashing of BigInt #394
* add averageFrom to DecayedValue #391
* Freshen up Applicative instances a bit #387
* less noise on DecayedValue tests #405
* Preparer #400
* Upgrade bijection to 0.7.2 #406

### Version 0.8.0 ###
* Removes deprecated monoid: https://github.com/twitter/algebird/pull/342
* Use some value classes: https://github.com/twitter/algebird/pull/340
* WIP: Algebird 210 211: https://github.com/twitter/algebird/pull/337
* Use Builder in Seq/List Monoids: https://github.com/twitter/algebird/pull/338
* Add a commment to close 334: https://github.com/twitter/algebird/pull/339
* Make trait public that should never have been private: https://github.com/twitter/algebird/pull/335
* Fix some issues with Successible/Predessible: https://github.com/twitter/algebird/pull/332
* Add Applicative and Functor as superclasses of Monad: https://github.com/twitter/algebird/pull/330
* Updated Maven section to updated version 0.7.0: https://github.com/twitter/algebird/pull/329

### Version 0.7.0 ###
* simplification for spacesaver. before buckets were kept in an option str...: https://github.com/twitter/algebird/pull/308
* Dynamic Summer, may use heuristics to decide not to keep a tuple in a buffer for aggregation.: https://github.com/twitter/algebird/pull/314
* Was a missing call to the update if we had flushed. Now we just do it as...: https://github.com/twitter/algebird/pull/324
* Bump to 2.10.4 in travis: https://github.com/twitter/algebird/pull/323
* Feature/auto formatter ran: https://github.com/twitter/algebird/pull/321
* Little commit fixing up some spacing per our norms:: https://github.com/twitter/algebird/pull/319
* provider methods for java: https://github.com/twitter/algebird/pull/317
* Serializable adaptive matrix: https://github.com/twitter/algebird/pull/318
* Clean up comments: https://github.com/twitter/algebird/pull/315
* make distribution immutable now that it contains mutable counters: https://github.com/twitter/algebird/pull/313
* a monoid that keeps track of the monoid usage: https://github.com/twitter/algebird/pull/311

### Version 0.6.0 ###
* make constructors for SpaceSaver subclasses public so that deserializers..: https://github.com/twitter/algebird/pull/289
* Restoring optimized SketchMapMonoid#sumOption: https://github.com/twitter/algebird/pull/293
* Newer caliper hll benchmark: https://github.com/twitter/algebird/pull/297
* hll optimization: https://github.com/twitter/algebird/pull/299
* Optimize the storage backend used in sketch map: https://github.com/twitter/algebird/pull/301
* migrate async summers from Summingbird: https://github.com/twitter/algebird/pull/296
* add sumOption to EventuallySemiGroup: https://github.com/twitter/algebird/pull/306
* Heavyhitters no longer attempts lazy storage in SketchMap: https://github.com/twitter/algebird/pull/305
* Async Maps performance improvements: https://github.com/twitter/algebird/pull/302
* Make the AsyncListSum be immutable again: https://github.com/twitter/algebird/pull/309

### Version 0.5.0 ###
* Remove handling that doesn't seem needed/wanted for longs: https://github.com/twitter/algebird/pull/287
* Add average() for DecayedValue: https://github.com/twitter/algebird/pull/286
* SketchMapParams can support being constructed with both width/depth and eps/delta: https://github.com/twitter/algebird/pull/285
* undo the undo of the interval fixes: https://github.com/twitter/algebird/pull/283
* Fix priority queue bug found by scalacheck: https://github.com/twitter/algebird/pull/280
* Adds type parameters to Intersection for more precision: https://github.com/twitter/algebird/pull/279
* Make resolver consistent with scalding: https://github.com/twitter/algebird/pull/276
* remove Math use in test: https://github.com/twitter/algebird/pull/275
* Remove extends function from algebird: https://github.com/twitter/algebird/pull/274
* remove unnecessary implicit requirement: https://github.com/twitter/algebird/pull/272
* Fix SketchMapMonoid zero: https://github.com/twitter/algebird/pull/269
* Make SketchMapMonoid params public: https://github.com/twitter/algebird/pull/265
* Add Predecessible and methods to Interval: https://github.com/twitter/algebird/pull/262
* Implement StreamSummary data structure for finding frequent/top-k items: https://github.com/twitter/algebird/pull/259

### Version 0.4.0 ###
* Make SketchMap1 the only SketchMap implementation: https://github.com/twitter/algebird/pull/256
* Use semigroup in StateWithError: https://github.com/twitter/algebird/pull/255
* Don't iterate through everything in unit monoid: https://github.com/twitter/algebird/pull/253
* Factor as much logic as possible into SketchmapMonoid1: https://github.com/twitter/algebird/pull/251
* Moving Trampoline flatMap implementation into trait: https://github.com/twitter/algebird/pull/249
* Integrate Caliper: https://github.com/twitter/algebird/pull/248
* Adds append method to MonoidAggregator and RingAggregator: https://github.com/twitter/algebird/pull/246
* Make the map monoid more performant for mutable maps: https://github.com/twitter/algebird/pull/245
* Make BFHash take on non negative values only: https://github.com/twitter/algebird/pull/243
* Fixed DecayedValue: https://github.com/twitter/algebird/pull/238
* Updated scaladoc to 0.3.0: https://github.com/twitter/algebird/pull/237
* Add Incrementable and tests: https://github.com/twitter/algebird/pull/234
* Updates sbt runner: https://github.com/twitter/algebird/pull/232
* Upgrade sbt, specs, add a build.properties, and bump travis's target: https://github.com/twitter/algebird/pull/231

### Version 0.3.1 ###
* Make a field transient in BloomFilter serialization: https://github.com/twitter/algebird/pull/209
* Add the TunnelMonoid to Util (like async function monoid): https://github.com/twitter/algebird/pull/213
* Make DecayedValueMonoid a class: https://github.com/twitter/algebird/pull/223
* SketchMap performance improvements: https://github.com/twitter/algebird/pull/227
* Add the Option group: https://github.com/twitter/algebird/pull/230
* Add Successible Typeclass: https://github.com/twitter/algebird/pull/234
* sumOption support on generated tuple Semigroups: https://github.com/twitter/algebird/pull/242

### Version 0.3.0 ###
* Optimize lots of the Monoids/Semigroups when summing many items: see Semigroup.sumOption https://github.com/twitter/algebird/pull/206
* Add many Aggregators for more convenient aggregation: https://github.com/twitter/algebird/pull/194
* Aggregators compose: https://github.com/twitter/algebird/pull/188
* Added scala.collection.Map (not just immutable.Map) algebras: https://github.com/twitter/algebird/pull/199
* Added Boolean monoids (Or and And): https://github.com/twitter/algebird/pull/198

### Version 0.2.0 ###

* Adds algebird-bijection module: https://github.com/twitter/algebird/pull/181
* Build cleanups: https://github.com/twitter/algebird/pull/180
* MapAlgebra.sparseEquiv: https://github.com/twitter/algebird/pull/182
* Remove isNonZero from Semigroup, add to Monoid: https://github.com/twitter/algebird/pull/183
* add MapAlgebra.mergeLookup: https://github.com/twitter/algebird/pull/185

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

---
layout: docs
title:  "Exponential Histogram"
section: "data"
source: "algebird-core/src/main/scala/com/twitter/algebird/ExpHist.scala"
scaladoc: "#com.twitter.algebird"
---

# Exponential Histogram

The `ExpHist` data structure implements the Exponential Histogram algorithm from [Maintaining Stream Statistics over Sliding Windows](http://www-cs-students.stanford.edu/~datar/papers/sicomp_streams.pdf), by Datar, Gionis, Indyk and Motwani.

An Exponential Histogram is a sliding window counter that can guarantee a bounded relative error. You configure the data structure with

- `epsilon`, the relative error you're willing to tolerate
- `windowSize`, the number of time ticks that you want to track

You interact with the data structure by adding (number, timestamp) pairs into the exponential histogram. querying it for an approximate counts with `guess`.

The approximate count is guaranteed to be within `conf.epsilon` relative error of the true count seen across the supplied `windowSize`.

## Example Usage

Let's set up a bunch of buckets to add into our exponential histogram. Each bucket tracks a delta and a timestamp. This example uses the same number for both, for simplicity.

```tut:book
import com.twitter.algebird.ExpHist
import ExpHist.{ Bucket, Config, Timestamp }

val maxTimestamp = 200
val inputs = (1 to maxTimestamp).map {
    i => ExpHist.Bucket(i, Timestamp(i))
  }.toVector

val actualSum = inputs.map(_.size).sum
```

Now we'll configure an instance of `ExpHist` to track the count and add each of our buckets in.

```tut:book
val epsilon = 0.01
val windowSize = maxTimestamp
val eh = ExpHist.empty(Config(epsilon, windowSize))
val full = inputs.foldLeft(eh) {
  case (histogram, Bucket(delta, timestamp)) => histogram.add(delta, timestamp)
}
```

Now we can query the full exponential histogram and compare the guess to the actual sum:

```tut:book
val approximateSum = full.guess
full.relativeError
val maxError = actualSum * full.relativeError

assert(full.guess <= actualSum + maxError)
assert(full.guess >= actualSum - maxError)
```

## l-Canonical Representation

The exponential histogram algorithm tracks buckets of size `2^i`. Every new increment to the histogram adds a bucket of size 1.

Because only `l` or `l+1` buckets of size `2^i` are allowed for each `i`, this increment might trigger an incremental merge of smaller buckets into larger buckets.

Let's look at 10 steps of the algorithm with `l == 2`:

```
1:  1 (1 added)
2:  1 1 (1 added)
3:  1 1 1 (1 added)
4:  1 1 2 (1 added, triggering a 1 + 1 = 2 merge)
5:  1 1 1 2 (1 added)
6:  1 1 2 2 (1 added, triggering a 1 + 1 = 2 merge)
7:  1 1 1 2 2 (1 added)
8:  1 1 2 2 2 (1 added, triggering a 1 + 1 = 2 merge AND a 2 + 2 = 4 merge)
9:  1 1 1 2 2 2 (1 added)
10: 1 1 2 2 4 (1 added, triggering a 1 + 1 = 2 merge AND a 2 + 2 = 4 merge)
```

Notice that the bucket sizes always sum to the algorithm step, ie `10 == 1 + 1 + 1 + 2 + 2 + 4`.

Now let's write out a list of the number of buckets of each size, ie `[bucketsOfSize(1), bucketsOfSize(2), bucketsOfSize(4), ....]`. Here's the above sequence in the new representation, plus a few more steps:

```
1:  1     <-- (l + 1)2^0 - l = 3 * 2^0 - 2 = 1
2:  2
3:  3
4:  2 1   <-- (l + 1)2^1 - l = 3 * 2^0 - 2 = 4
5:  3 1
6:  2 2
7:  3 2
8:  2 3
9:  3 3
10: 2 2 1 <-- (l + 1)2^2 - l = 3 * 2^0 - 2 = 10
11: 3 2 1
12: 2 3 1
13: 3 3 1
14: 2 2 2
15: 3 2 2
16: 2 3 2
16: 3 3 2
17: 2 2 3
```

This sequence is called the "l-canonical representation" of `s`.

A pattern emerges! Every bucket size except the largest looks like a binary counter (if you added `l + 1` to the bit, and made the counter little-endian). Let's call this the "binary" prefix, or `bin(_)`.

Here's the above sequence with the prefix decoded from "binary":

```
1:  1        <-- (l + 1)2^0 - l = 3 * 2^0 - 2 = 1
2:  2
3:  3

4:  bin(0) 1 <-- (l + 1)2^1 - l = 3 * 2^0 - 2 = 4
5:  bin(1) 1
6:  bin(0) 2
7:  bin(1) 2
8:  bin(0) 3
9:  bin(1) 3

10: bin(0) 1 <-- (l + 1)2^2 - l = 3 * 2^0 - 2 = 10
11: bin(1) 1
12: bin(2) 1
13: bin(3) 1
14: bin(0) 2
15: bin(1) 2
16: bin(2) 2
16: bin(3) 2
17: bin(0) 3
```

Some observations about the pattern:

The l-canonical representation groups the natural numbers into groups of size `(l + 1)2^i` for `i >= 0`.

Each group starts at `(l + 1)2^i - l` (see 1, 4, 10... above)

Within each group, the "binary" prefix of the l-canonical rep cycles from `0` to `(2^i - 1)`, `l + 1` total times. (This makes sense; each cycle increments the final entry by one until it hits `l + 1`; after that an increment triggers a merge and a new "group" begins.)

The final l-canonical entry == `floor((position within the group) / 2^i)`, or the "quotient" of that position and `2^i`.

That's all we need to know to write a procedure to generate the l-canonical representation! Here it is again:

## L-Canonical Representation Procedure

- Find the largest `j` s.t. `2^j <= (s + l) / (1 + l)`
- let `s' := 2^j(1 + l) - l`

(`s'` is the position if the start of a group, ie 1, 4, 10...)

- `diff := (s - s')` is the position of s within that group.
- let `b :=` the little-endian binary rep of `diff % (2^j - 1)`
- let `ret :=` return vector of length `j`:


```scala
(0 until j).map { i => ret(i) = b(i) + l }
ret(j) = math.floor(diff / 2^j)
```

---
layout: docs
title:  "Averaged Value"
section: "data"
source: "algebird-core/src/main/scala/com/twitter/algebird/AveragedValue.scala"
scaladoc: "#com.twitter.algebird.AveragedValue"
---

# Averaged Value

The `AveragedValue` data structure allows you to keep track of the count and mean of a stream of numbers with only one pass over the data.

## Algebraic Properties

Here's a note on how to add them.

Stability... so the online algorithm here: <https://en.wikipedia.org/wiki/Algorithms_for_calculating_variance#Online_algorithm>

is numerically stable when the counts are really unequal. "This algorithm is much less prone to loss of precision due to [catastrophic cancellation](https://en.wikipedia.org/wiki/Loss_of_significance)"

This section on the parallel algorithm: <https://en.wikipedia.org/wiki/Algorithms_for_calculating_variance#Parallel_algorithm>

shows the actual algorithm that we use here.... BUT it's numerically unstable when the counts are close to each other and both large, because the numerical error in the difference of the means doesn't get scaled down. In such cases, says wiki, prefer the vanilla version. And that's exactly what we do.

Also note that it's a group. You can subtract values, or negate the thing.

### Notes

This means that for big data we won't overflow. If the right side is small, < 2^63 items, ie within a long, you'll use the special algo.

If the numbers are roughly equal then you MIGHT get an overflow. But you're often not adding multiple enormous numbers (~2^62).

Numerically stable is key too.

## Usage Examples

- plus, minus, negative
- use it to fold a bunch of longs
- show the aggregator

## Related Data Structures

- decayedvalue
- moments

### Links

- Source: [AveragedValue.scala](https://github.com/twitter/algebird/blob/develop/algebird-core/src/main/scala/com/twitter/algebird/AveragedValue.scala)
- Tests: [AveragedValueLaws.scala](https://github.com/twitter/algebird/blob/develop/algebird-test/src/test/scala/com/twitter/algebird/AveragedValueLaws.scala)
- Scaladoc: [AveragedValue]({{site.baseurl}}/api#com.twitter.algebird.AveragedValue)

### Documentation Help

We'd love your help fleshing out this documentation! You can edit this page in your browser by clicking [this link](https://github.com/twitter/algebird/edit/develop/docs/src/main/tut/datatypes/averaged_value.md).

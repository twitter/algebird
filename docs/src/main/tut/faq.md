---
layout: page
title:  "FAQ"
section: "faq"
position: 4
---

## Frequently Asked Questions

* [Why not use spire?](#spire)
* [Why not use Scalaz's Monoid trait?](#scalaz)
* [How can I help?](#contributing)

### <a id="spire" href="#spire"></a>Why not use Spire?

We didn't know about it when we started this code, but it seems like we're more focused on large scale analytics.

### <a id="scalaz" href="#scalaz"></a>Why not use Scalaz's [Monoid](http://docs.typelevel.org/api/scalaz/stable/7.0.4/doc/#scalaz.Monoid) trait?

The answer is a mix of the following:

- The trait itself is tiny, we just need zero and plus, it is the implementations for all the types that are important. We wrote a code generator to derive instances for all the tuples, and by hand wrote monoids for List, Set, Option, Map, and several other objects used for counting (DecayedValue for exponential decay, AveragedValue for averaging, HyperLogLog for approximate cardinality counting). It's the instances that are useful in scalding and elsewhere.
- We needed this to work in scala 2.8, and it appeared that Scalaz 7 didn't support 2.8. We've since moved to 2.9, though.
- We also needed Ring and Field, and those are not (as of the writing of the code) in Scalaz.
- If you want to interop, it is trivial to define implicit conversions to and from Scalaz Monoid.

### <a id="contributing" href="#contributing"></a>How can I help?

The Algebird community welcomes and encourages contributions! Here are a few ways to help out:

- Look at the [code coverage report](https://codecov.io/github/twitter/algebird?branch=develop), find some untested code, and write a test for it. Even simple helper methods and syntax enrichment should be tested.
- Find an [open issue](https://github.com/twitter/algebird/issues?q=is%3Aopen+is%3Aissue), leave a comment on it to let people know you are working on it, and submit a pull request.

See the [contributing guide]({{ site.baseurl }}/contributing.html) for more information.

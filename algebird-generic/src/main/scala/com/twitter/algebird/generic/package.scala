package com.twitter.algebird

/**
 * This package introduces a number of general-purpose combinators for
 * combining Aggregator instances. The goal here is to remove as much
 * use-site boilerplate from aggregator definitions as possible (by
 * stashinng it in the combinators).
 *
 * The most interesting thing going on is the use of heterogeneous lists
 * instead of tuples for collections of values. The advantage here is
 * that we can express "concatenations" of HLists at the type-level.
 *
 * Aggregators are defined by three types:
 *
 *  - input type (A)
 *  - aggregated type (B)
 *  - output type (C)
 *
 * We identified five basic shapes, three of which have product/coproduct
 * variants, and two of which don't. The diagrams here show how input,
 * aggregated, and output types are related by these combinators:
 *
 * Applicative:
 *       / b1 -> c1
 *   a  -> b2 -> c2
 *       \ b3 -> c3
 *
 * Parallel (product/coproduct):
 *   a1 -> b1 -> c1
 *   a2 -> b2 -> c2
 *   a3 -> b3 -> c3
 *
 * Joined (product/coproduct):
 *   a1 -> b1 \
 *   a2 -> b2 -> c
 *   a3 -> b3 /
 *
 * Cinched (product/coproduct):
 *   a1 \      / c1
 *   a2 -> b* -> c2
 *   a3 /      \ c3
 *
 * Combined:
 *       / b1 \
 *   a  -> b2 -> c
 *       \ b3 /
 */
package object generic

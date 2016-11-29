---
layout: docs
title:  "First and Last"
section: "data"
---

# First and Last

`First[T]` and `Last[T]` are data structures that keep track of, respectively, the earliest and latest instances of `T` that you've seen. `First[T]` works for any type `T`:

```tut:book
import com.twitter.algebird.{ First, Last }
First(3) + First(2) + First(1)
First("a") + First("b") + First("c")
```

As does `Last[T]`:

```tut:book
Last(3) + Last(2) + Last(1)
Last("a") + Last("b") + Last("c")
```

## Algebraic Properties

`First[T]` and `Last[T]` are both non-commutative semigroups. For `First[T]`, the `+` function keeps the left input, while `Last[T]`'s `+` implementation keeps the right input. For example, for `First[T]`:

```tut:book
val first1 = First(1) + First(3) == First(1)
val first3 = First(3) + First(1) == First(3)
assert(first1 && first3)
```

And for `Last[T]`:

```tut:book
val last3 = Last(1) + Last(3) == Last(3)
val last1 = Last(3) + Last(1) == Last(1)
assert(last3 && last1)
```

## Usage Examples

Let's use `First[T]` and `Last[T]` to keep track of the first and last username that a Twitter user has followed over the lifetime of their account. First let's define a type alias for `Username`:

```tut:book
type Username = String
```

To track `First` and `Last` simultaneously we'll use a combinator. As discussed on the [Product Algebra docs page](combinator/product_algebra.html), the `Tuple2[A, B]` semigroup works by separately combining its left and right elements. This means that we can use a pair - a `(First[Username], Last[Username])` - to track both the oldest and most recent twitter username that we've seen.

```tut:book
def follow(user: Username): (First[Username], Last[Username]) =
  (First(user), Last(user))
```

Now let's "add" up a few of these pairs, using the semigroup. First, we'll import Algebird's `Operators._`, which will enrich any semigroup with a `+` method.

```tut:book
import com.twitter.algebird.Operators._

val follows = follow("sam") + follow("erik") + follow("oscar") + follow("kelley")
(First("sam"), Last("kelley")) == follows
```

## Related Data Structures

`First[T]` and `Last[T]` are related to `Min[T]` and `Max[T]`. All four of these data structures wrap up instances of another type `T` to make them combine in a different way. `First[T]` and `Last[T]` force them to combine based on the order that they're seen in a stream. `Min[T]` and `Max[T]` force their combination to depend on the ordering of the type `T`.

See [the docs on `Min` and `Max`](min_and_max.html) for more information.

### Links

- Source: [First.scala](https://github.com/twitter/algebird/blob/develop/algebird-core/src/main/scala/com/twitter/algebird/First.scala) and [Last.scala](https://github.com/twitter/algebird/blob/develop/algebird-core/src/main/scala/com/twitter/algebird/Last.scala)
- Tests: [FirstSpec.scala](https://github.com/twitter/algebird/blob/develop/algebird-test/src/test/scala/com/twitter/algebird/FirstSpec.scala) and [LastSpec.scala](https://github.com/twitter/algebird/blob/develop/algebird-test/src/test/scala/com/twitter/algebird/LastSpec.scala)
- Scaladoc: [First]({{site.baseurl}}/api#com.twitter.algebird.First) and [Last]({{site.baseurl}}/api#com.twitter.algebird.Last)

### Documentation Help

We'd love your help fleshing out this documentation! You can edit this page in your browser by clicking [this  link](https://github.com/twitter/algebird/edit/develop/docs/src/main/tut/datatypes/first_and_last.md).

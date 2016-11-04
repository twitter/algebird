# Abstract Algebra

Before getting into the details of Algebird, let's go through a few simple abstract algebra definitions.  Folks interested in more details may wish to refer to the excellent [*Notes on Abstract Algebra*](http://www.math.usm.edu/perry/old_classes/mat423fa11/notes_25aug2011.pdf) from University of Southern Mississipi professor John Perry.

## Semigroup

Given a set S and an operation `+`, we say that `(S, +)` is a *semigroup* if it satisfies the following properties for any x, y, z &isin; S:

- *Closure*: `x + y` &isin; S
- *Associativity*: `(x + y) + z = x + (y + z)`

We also say that *S forms a semigroup under **.

#### Examples of Semigroups

- Strings form a semigroup under concatenation.

## Monoids

A monoid is a semigroup with an identity element.  More formally, given a set M and an operation `+`, we say that `(M, +)` is a *monoid* if it satisfies the following properties for any x, y, z &isin; M:

- *Closure*: `x + y` &isin; M
- *Associativity*: `(x + y) + z = x + (y + z)`
- *Identity*: There exists an e &isin; M such that `e + x = x + e = x`

We also say that *M is a monoid under +.*

#### Examples of Monoids

- The natural numbers *N* are monoids under addition
- *N* is a monoid under multiplication.
- Strings form a monoid under concatenation (`""` is the identity element)

## Groups

We say that `(G, *)` is a *group* if it is a Monoid that also satisfies the following property:

- *Invertibility*: For every x &isin; G there is an xinv such that `x * xinv = xinv * x = e`

Moreover, it is an *abelian group* if it satisfies the property:

- *Commutative*: `x * y = y * x` for all x and y &isin; G.

#### Examples of Groups

- Integers *Z* are an abelian group under addition
- Natural numbers are *not* a group under addition (given a number *x* in *N*, *-x* is not in *N*)
- Neither integers nor natural numbers are a group under multiplication, but the set of nonzero rational numbers (`n/d` for any `n, d` &isin; `N`, `n` &ne; 0, `d` &ne; 0) is an (abelian) group under multiplication.

## Rings

Whereas a group is defined by a set and a single operation, a ring is defined by a set and two operations.  Given a set *R* and operations `*` and `+`, we say that `(R, +, *)` is a ring if it satisfies the following properties:

- `(R, +)` is an abelian group
- For any x and y &isin; R, `x * y` &isin; R.
- For any x, y, and z &isin; R, `(x * y) * z = x * (y * z)`.
- For any x, y, and z &isin; R, `x * (y + z) = x * y + x * z` and `(x + y) * z = x * z + y * z`

Notes on rings:

- A ring necessarily has an identity under `+` (called the *additive identity*), typically referred to as *zero*, **0**).
- A ring does not necessarily have an identity under `*` (called the *multiplicative identity*).
- A ring is not necessarily commutitive under `*`.
- A ring does not necessarily satisfy the inverse property under `*`.
- There is not necessarily any special property of the additive identity under multiplication.

A few other types of rings:

- We say that `(R, +, *)` is a *ring with unity* if it has a multiplicative identity, referred to as *one*, or **1**.
- We say that `(R, +, *)` is a *commutative ring* if it satisfies the commutative property under multiplication, that is `x * y = y * x` &forall; x,y &isin; R.

#### Examples of rings

- `(Z, +, *)`
- The set of square square matrices of a given size are a ring.

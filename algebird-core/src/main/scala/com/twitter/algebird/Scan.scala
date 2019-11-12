package com.twitter.algebird

import scala.collection.compat._

object Scan {

  /**
   * Most consumers of Scan don't care about the type of the type State type variable. But for those that do,
   * we make an effort to expose it in all of our combinators.
   * @tparam I
   * @tparam S
   * @tparam O
   */
  type Aux[-I, S, +O] = Scan[I, O] { type State = S }

  implicit def applicative[I]: Applicative[({ type L[O] = Scan[I, O] })#L] = new ScanApplicative[I]

  def from[I, S, O](initState: S)(presentAndNextStateFn: (I, S) => (O, S)): Aux[I, S, O] =
    new Scan[I, O] {
      override type State = S
      override val initialState = initState
      override def presentAndNextState(i: I, s: State): (O, State) = presentAndNextStateFn(i, s)
    }

  def fromFunction[I, O](f: I => O): Aux[I, Unit, O] = new Scan[I, O] {
    override type State = Unit
    override val initialState = ()
    override def presentAndNextState(i: I, stateBeforeProcessingI: Unit): (O, State) = (f(i), ())
  }

  /**
   * Scans take streams of inputs to streams of outputs, but some scans have trivial inputs and just produce a stream of
   * outputs. Streams can be thought of as being a hidden state that is queryable for a head element, and another hidden
   * state that represents the rest of the stream.
   * @param initState The initial state of the scan; think of this as an infinite stream.
   * @param destructor This function decomposes a stream into the its head-element and tail-stream.
   * @tparam S The hidden state of the stream that we are turning into a Scan.
   * @tparam O The type of the elments of the stream that we are turning into a Scan
   * @return A Scan whose inputs are irrelevant, and whose outputs are those that we would get from implementing
   *         a stream using the information provided to this method.
   */
  def iterate[S, O](initState: S)(destructor: S => (O, S)): Aux[Any, S, O] = new Scan[Any, O] {
    override type State = S
    override val initialState = initState
    override def presentAndNextState(i: Any, stateBeforeProcessingI: S): (O, S) =
      destructor(stateBeforeProcessingI)
  }

  /**
   * A Scan that returns the number N for the Nth input (starting from 0)
   */
  val index: Aux[Any, Long, Long] = iterate(0L)(n => (n, n + 1))

  def identity[A] = fromFunction[A, A](x => x)

  /**
   *
   * @param initStateCreator A call-by-name method that allocates new mutable state
   * @param presentAndUpdateStateFn A function that both presents the output value, and has the side-effect of updating the mutable state
   * @tparam I
   * @tparam S
   * @tparam O
   * @return A Scan that safely encapsulates state while it's doing its thing.
   */
  def mutable[I, S, O](initStateCreator: => S)(presentAndUpdateStateFn: (I, S) => O): Aux[I, S, O] =
    new Scan[I, O] {
      override type State = S
      override def initialState = initStateCreator
      override def presentAndNextState(i: I, s: S): (O, S) = (presentAndUpdateStateFn(i, s), s)
    }

  /**
   * The trivial scan that always returns the same value, regardless of input
   * @param t
   * @tparam T
   */
  def const[T](t: T): Aux[Any, Unit, T] = fromFunction(_ => t)

  /**
   *
   * @param aggregator
   * @param initState
   * @tparam A
   * @tparam B
   * @tparam C
   * @return A scan which, when given [a_1, ..., a_n] outputs [c_1, ..., c_n] where
   *         c_i = initState + aggregator.prepare(a_1) + ... + aggregator.prepare(a_i)
   */
  def fromAggregator[A, B, C](aggregator: Aggregator[A, B, C], initState: B): Aux[A, B, C] =
    from(initState) { (a: A, stateBeforeProcessingI: B) =>
      // nb: the order of the arguments to semigroup.plus here is what determines the order of the final summation;
      // this matters because not all semigroups are commutative
      val stateAfterProcessingA =
        aggregator.semigroup.plus(stateBeforeProcessingI, aggregator.prepare(a))
      (aggregator.present(stateAfterProcessingA), stateAfterProcessingA)
    }

  /**
   *
   * @param monoidAggregator
   * @tparam A
   * @tparam B
   * @tparam C
   * @return A scan which, when given [a_1, ..., a_n] outputs [c_1, ..., c_n] where
   *         c_i = monoidAggregator.monoid.zero + aggregator.prepare(a_1) + ... + aggregator.prepare(a_i)
   */
  def fromMonoidAggregator[A, B, C](monoidAggregator: MonoidAggregator[A, B, C]): Aux[A, B, C] =
    fromAggregator(monoidAggregator, monoidAggregator.monoid.zero)

}

/**
 * The Scan trait is an alternative to the "scanLeft" method on iterators/other collections for a range of
 * of use-cases where scanLeft is awkward to use. At a high level it provides some of the same functionality as
 * [[scala.collection.Iterator.scanLeft]], but with a separation of "what is the state of the scan" from
 * "what are the elements that I'm scanning over?". In particular, when scanning over an iterator with `N` elements,
 * the output is an iterator with `N` elements (in contrast to scanLeft's `N+1`).
 *
 * If you find yourself writing a `scanLeft` over pairs of elements, where you only use one element of the pair within
 * the `scanLeft` itself then throw that element away in a `map` immediately after the scanLeft is done, then this
 * abstraction is for you.
 *
 * The canonical method to use a scanner is its `apply` method.
 *
 *
 * @tparam I The type of elements that the computation is scanning over.
 * @tparam O The output type of the scan (typically distinct from the hidden `State` of the scan.
 */
sealed trait Scan[-I, +O] extends Serializable { self =>

  import Scan.{from, Aux}

  /**
   * The computation of any given scan involves keeping track of a hidden state.
   */
  type State

  /**
   * The state of the scan before any elements have been processed
   * @return
   */
  def initialState: State

  /**
   *
   * @param i An element in the stream to process
   * @param stateBeforeProcessingI The state of the scan before processing i
   * @return The output of the scan corresponding to processing i with state stateBeforeProcessing,
   *         along with the result of updating stateBeforeProcessing with the information from i.
   */
  def presentAndNextState(i: I, stateBeforeProcessingI: State): (O, State)

  /**
   * @param iter
   * @return If iter = Iterator(a_1, ..., a_n), return:
   * Iterator(o_1, ..., o_n) where
   * (o_(i+1), state_(i+1)) = presentAndNextState(a_i, state_i)
   * and state_0 = initialState
   *
   */
  def scanIterator(iter: Iterator[I]): Iterator[O] = new AbstractIterator[O] {
    override def hasNext: Boolean = iter.hasNext
    var state: State = initialState
    override def next: O = {
      val thisState = state
      val thisA = iter.next
      val (thisC, nextState) = presentAndNextState(thisA, thisState)
      state = nextState
      thisC
    }
  }

  /**
   * @param inputs
   * @param bf
   * @tparam In The type of the input collection
   * @tparam Out The type of the output collection
   * @return
   * Given inputs as a collection of the form `[a_1, ..., a_n]` the output will be a collection of the form:
   * `[o_1, ..., o_n]` where
   * `(o_(i+1), state_(i+1)) = presentAndNextState(a_i, state_i)`
   * and `state_0 = initialState`.
   */
  def apply[In <: TraversableOnce[I], Out](
      inputs: In
  )(implicit bf: BuildFrom[In, O, Out]): Out =
    bf.fromSpecific(inputs)(scanIterator(inputs.toIterator))

  // combinators

  def replaceState(newInitialState: => State): Aux[I, State, O] =
    from(newInitialState)(presentAndNextState(_, _))

  def composePrepare[I1](f: I1 => I): Aux[I1, State, O] = from(initialState) { (i, stateBeforeProcessingI) =>
    presentAndNextState(f(i), stateBeforeProcessingI)
  }

  def andThenPresent[O1](g: O => O1): Aux[I, State, O1] = from(initialState) { (i, stateBeforeProcessingI) =>
    val (c, stateAfterProcessingA) = presentAndNextState(i, stateBeforeProcessingI)
    (g(c), stateAfterProcessingA)
  }

  /**
   *
   * @tparam I1
   * @return If this Scan's `apply` method is given inputs `[a_1, ..., a_n]` resulting in outputs
   * of the form [o_1, ..., o_n], then {{joinWithInput}} results in a Scan whose `apply` method
   * returns [(a_1, o_1), ..., (a_n, o_n)] when given the same input.
   * In other words, `joinWithInput` returns a scanner that is semantically identical to
   * `this.join(Scan.identity[I1]`, but where we don't pollute the `State` by pairing it
   * redundantly with `Unit`.
   */
  def joinWithInput[I1 <: I]: Aux[I1, State, (I1, O)] = from(initialState) { (i, stateBeforeProcessingI) =>
    val (o, stateAfterProcessingI) = self.presentAndNextState(i, stateBeforeProcessingI)
    ((i, o), stateAfterProcessingI)
  }

  /**
   * If this Scan's `apply` method is given inputs [a_1, ..., a_n] resulting in outputs
   * of the form [o_1, ..., o_n], where (o_(i+1), state_(i+1)) = presentAndNextState(a_i, state_i)
   *  and state_0 = initialState:
   * @return A scan that whose apply method, when given inputs [a_1, ..., a_n] will return
   * [(o_1, state_0), ..., (o_n, state_(n-1))].
   */
  def joinWithPriorState: Aux[I, State, (State, O)] = from(initialState) { (i, stateBeforeProcessingI) =>
    val (o, stateAfterProcessingA) = self.presentAndNextState(i, stateBeforeProcessingI)
    ((stateBeforeProcessingI, o), stateAfterProcessingA)
  }

  /**
   * If this Scan's `apply` method is given inputs [a_1, ..., a_n] resulting in outputs
   * of the form [o_1, ..., o_n], where (o_(i+1), state_(i+1)) = presentAndNextState(a_i, state_i)
   *  and state_0 = initialState:
   * @return A scan that whose apply method, when given inputs [a_1, ..., a_n] will return
   * [(o_1, state_1), ..., (o_n, state_n].
   */
  def joinWithPosteriorState: Aux[I, State, (O, State)] = from(initialState) { (i, stateBeforeProcessingI) =>
    val (c, stateAfterProcessingA) = self.presentAndNextState(i, stateBeforeProcessingI)
    ((c, stateAfterProcessingA), stateAfterProcessingA)
  }

  /**
   * If this Scan's `apply` method is given inputs [a_1, ..., a_n] resulting in outputs
   * of the form [o_1, ..., o_n]
   * @return A scan that whose apply method, when given inputs [a_1, ..., a_n] will return
   * [(o_1, 1), ..., (o_n, n)].
   * In other words: `scan.joinWithIndex(foo) == scan(foo).zipWithIndex)`
   */
  def joinWithIndex: Aux[I, (State, Long), (O, Long)] = join(Scan.index)

  /**
   * @param scan2
   * @tparam I2
   * @tparam O2
   * @return f this Scan's apply method is given inputs [a_1, ..., a_n] resulting in outputs of
   * the form [o_1, ..., o_n], and scan2.apply([b_1, ..., b_n] = [p_1, ..., p_n] then
   * `zip` will return a scan whose apply method, when given input
   * [(a_1, b_1), ..., (a_n, b_n)] results in the output [(o_1, p_1), ..., (o_2, p_2)].
   * In other words: `scan.zip(scan2)(foo.zip(bar)) == scan(foo).zip(scan2(bar)) `
   */
  def zip[I2, O2](scan2: Scan[I2, O2]): Aux[(I, I2), (State, scan2.State), (O, O2)] =
    from((self.initialState, scan2.initialState)) { (i1i2, stateBeforeProcessingI1I2) =>
      val (o1, state1AfterProcesingI1) =
        self.presentAndNextState(i1i2._1, stateBeforeProcessingI1I2._1)
      val (o2, state2AfterProcesingI2) =
        scan2.presentAndNextState(i1i2._2, stateBeforeProcessingI1I2._2)
      ((o1, o2), (state1AfterProcesingI1, state2AfterProcesingI2))
    }

  /**
   * @param scan2
   * @tparam I2
   * @tparam O2
   * @return  If this Scan's apply method is given inputs [a_1, ..., a_n] resulting in outputs of
   * the form [o_1, ..., o_n], and scan2.apply([a_1, ..., a_n] = [p_1, ..., p_n] then
   * `join` will return a scan whose apply method returns [(o_1, p_1), ..., (o_2, p_2)].
   * In other words: `scan.join(scan2)(foo) == scan(foo).zip(scan2(foo)) `
   */
  def join[I2 <: I, O2](scan2: Scan[I2, O2]): Aux[I2, (State, scan2.State), (O, O2)] =
    from((self.initialState, scan2.initialState)) { (i, stateBeforeProcessingI) =>
      val (o1, state1AfterProcesingI1) = self.presentAndNextState(i, stateBeforeProcessingI._1)
      val (o2, state2AfterProcesingI2) = scan2.presentAndNextState(i, stateBeforeProcessingI._2)
      ((o1, o2), (state1AfterProcesingI1, state2AfterProcesingI2))
    }

  /**
   * Takes the output of this scan and feeds as input into scan2.
   * @param scan2
   * @tparam P
   * @return If this Scan's apply method is given inputs [a_1, ..., a_n] resulting in outputs of
   * the form [o_1, ..., o_n], and scan2.apply([o_1, ..., o_n] = [p_1, ..., p_n] then
   * `compose` will return a scan which returns [p_1, ..., p_n].
   */
  def compose[P](scan2: Scan[O, P]): Aux[I, (State, scan2.State), P] =
    from((self.initialState, scan2.initialState)) { (i, stateBeforeProcessingI) =>
      val (o, state1AfterProcesingI) = presentAndNextState(i, stateBeforeProcessingI._1)
      val (p, state2AfterProcesingO) = scan2.presentAndNextState(o, stateBeforeProcessingI._2)
      (p, (state1AfterProcesingI, state2AfterProcesingO))
    }

}

class ScanApplicative[I] extends Applicative[({ type L[O] = Scan[I, O] })#L] {
  override def map[T, U](mt: Scan[I, T])(fn: T => U): Scan[I, U] =
    mt.andThenPresent(fn)

  override def apply[T](v: T): Scan[I, T] =
    Scan.const(v)

  override def join[T, U](mt: Scan[I, T], mu: Scan[I, U]): Scan[I, (T, U)] =
    mt.join(mu)
}

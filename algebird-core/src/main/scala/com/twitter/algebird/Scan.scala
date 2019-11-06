package com.twitter.algebird

import scala.collection.AbstractIterator
import scala.collection.generic.CanBuildFrom

object Scan {
  implicit def applicative[I]: Applicative[({ type L[O] = Scan[I, O] })#L] = new ScanApplicative[I]

  def from[I, S, O](initState: S)(presentAndNextStateFn: (I, S) => (O, S)): Scan[I, O] =
    new Scan[I, O] {
      override type State = S
      override val initialState = initState
      override def presentAndNextState(i: I, s: State): (O, State) = presentAndNextStateFn(i, s)
    }

  /**
   *
   * @param initStateCreator A call-by-name method that allocates new mutable state
   * @param presentAndUpdateStateFn A function that both presents the output value, and has the side-effect of updating the mutable state
   * @tparam I
   * @tparam S
   * @tparam O
   * @return A Scan that safely encapsulates state while it's doing its thing.
   */
  def mutable[I, S, O](initStateCreator: => S)(presentAndUpdateStateFn: (I, S) => O): Scan[I, O] =
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
  def const[T](t: T): Scan[Any, T] = from(()) { (_, _) =>
    (t, ())
  }

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
  def fromAggregator[A, B, C](aggregator: Aggregator[A, B, C], initState: B): Scan[A, C] =
    new Scan[A, C] {
      override type State = B

      override val initialState = initState

      override def presentAndNextState(a: A, stateBeforeProcessingI: B): (C, B) = {
        // nb: the order of the arguments to semigroup.plus here is what determines the order of the final summation;
        // this matters because not all semigroups are commutative
        val stateAfterProcessingA =
          aggregator.semigroup.plus(stateBeforeProcessingI, aggregator.prepare(a))
        (aggregator.present(stateAfterProcessingA), stateAfterProcessingA)
      }
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
  def fromMonoidAggregator[A, B, C](monoidAggregator: MonoidAggregator[A, B, C]): Scan[A, C] =
    fromAggregator(monoidAggregator, monoidAggregator.monoid.zero)

  /**
   *
   * @param aggregator
   * @param initState
   * @tparam A
   * @tparam B
   * @tparam C
   * @return  A scan which, when given [a_1, ..., a_n] outputs [c_1, ..., c_n] where
   *         c_i = aggregator.prepare(a_i) + ... + aggregator.prepare(a_1) + initState
   */
  def fromAggregatorReverse[A, B, C](aggregator: Aggregator[A, B, C], initState: B): Scan[A, C] =
    new Scan[A, C] {
      override type State = B

      override val initialState = initState

      override def presentAndNextState(a: A, stateBeforeProcessingI: B): (C, B) = {
        // nb: the order of the arguments to semigroup.plus here is what determines the order of the final summation;
        // this matters because not all semigroups are commutative
        val stateAfterProcessingA =
          aggregator.semigroup.plus(aggregator.prepare(a), stateBeforeProcessingI)
        (aggregator.present(stateAfterProcessingA), stateAfterProcessingA)
      }
    }

  /**
   *
   * @param monoidAggregator
   * @tparam A
   * @tparam B
   * @tparam C
   * @return  A scan which, when given [a_1, ..., a_n] outputs [c_1, ..., c_n] where
   *         c_i = aggregator.prepare(a_i) + ... + aggregator.prepare(a_1) + monoidAggregator.monoid.zero
   */
  def fromMonoidAggregatorReverse[A, B, C](
      monoidAggregator: MonoidAggregator[A, B, C]
  ): Scan[A, C] =
    fromAggregatorReverse(monoidAggregator, monoidAggregator.monoid.zero)

}

/**
 * The Scan trait is an alternative to the "scanLeft" method on iterators/other collections for a range of
 * of use-cases where scanLeft is awkward to use. At a high level it provides some of the same functionality as scanleft,
 * but with a separation of "what is the state of the scan" from "what are the elements that I'm scanning over?"
 * In particular, when scanning over an iterator with N elements, the output is an iterator with N elements (in contrast
 * to scanLeft's N+1).
 *
 * If you find yourself writing a ScanLeft over pairs of elements, where you only use one element of the pair within
 * the scanLeft itself then throw that element away in a "map" immediately after the scanLeft is done, then this
 * abstraction is for you.
 *
 *
 * @tparam I The type of elements that the computation is scanning over.
 * @tparam O The output type of the scan (typically distinct from the hidden {{{State}}} of the scan.
 */
sealed trait Scan[-I, +O] extends Serializable { self =>

  /**
   * The computation of any given scan involves keeping track of a hidden state of type {{{State}}}
   */
  type State

  /**
   *
   * @return The state of the scan before any elements have been processed
   */
  def initialState: State

  /**
   *
   * @param i An element in the stream to process
   * @param stateBeforeProcessingI The state of the scan before processing {{{i}}}
   * @return The output of the scan corresponding to processing {{{i}}} with state {{{stateBeforeProcessing}}},
   *         along with the result of updating {{{stateBeforeProcessing}}} with the information from {{{i}}}.
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
   * Given inputs as a collection of the form [a_1, ..., a_n] the output will be a collection of the form:
   * [o_1, ..., o_n] where
   * (o_(i+1), state_(i+1)) = presentAndNextState(a_i, state_i)
   * and state_0 = initialState
   */
  def apply[In <: TraversableOnce[I], Out](
      inputs: In
  )(implicit bf: CanBuildFrom[In, O, Out]): Out = {
    val builder = bf()
    builder ++= scanIterator(inputs.toIterator)
    builder.result
  }

  // combinators

  def replaceState(newInitialState: => State): Scan[I, O] = new Scan[I, O] {
    override type State = self.State

    override def initialState: State = newInitialState

    override def presentAndNextState(i: I, stateBeforeProcessingI: State): (O, State) =
      self.presentAndNextState(i, stateBeforeProcessingI)
  }

  def composePrepare[I1](f: I1 => I): Scan[I1, O] = new Scan[I1, O] {
    override type State = self.State

    override def initialState: State = self.initialState

    override def presentAndNextState(i: I1, stateBeforeProcessingI: State): (O, State) =
      self.presentAndNextState(f(i), stateBeforeProcessingI)
  }

  def andThenPresent[O1](g: O => O1): Scan[I, O1] = new Scan[I, O1] {
    override type State = self.State
    override def initialState: State = self.initialState

    override def presentAndNextState(i: I, stateBeforeProcessingI: State): (O1, State) = {
      val (c, stateAfterProcessingA) = self.presentAndNextState(i, stateBeforeProcessingI)
      (g(c), stateAfterProcessingA)
    }
  }

  def zipWithInput[I1 <: I]: Scan[I1, (I1, O)] = new Scan[I1, (I1, O)] {
    override type State = self.State

    override def initialState: State = self.initialState

    override def presentAndNextState(i: I1, stateBeforeProcessingI: State): ((I1, O), State) = {
      val (o, stateAfterProcessingA) = self.presentAndNextState(i, stateBeforeProcessingI)
      ((i, o), stateAfterProcessingA)
    }
  }

  def zipWithPriorState: Scan[I, (State, O)] = new Scan[I, (State, O)] {
    override type State = self.State

    override def initialState: State = self.initialState

    override def presentAndNextState(i: I, stateBeforeProcessingI: State): ((State, O), State) = {
      val (o, stateAfterProcessingA) = self.presentAndNextState(i, stateBeforeProcessingI)
      ((stateBeforeProcessingI, o), stateAfterProcessingA)
    }
  }

  def zipWithPosteriorState: Scan[I, (O, State)] = new Scan[I, (O, State)] {
    override type State = self.State

    override def initialState: State = self.initialState

    override def presentAndNextState(i: I, stateBeforeProcessingI: State): ((O, State), State) = {
      val (c, stateAfterProcessingA) = self.presentAndNextState(i, stateBeforeProcessingI)
      ((c, stateAfterProcessingA), stateAfterProcessingA)
    }
  }

  def zip[I2, O2](scan2: Scan[I2, O2]): Scan[(I, I2), (O, O2)] = new Scan[(I, I2), (O, O2)] {
    override type State = (self.State, scan2.State)

    override def initialState: State = (self.initialState, scan2.initialState)

    override def presentAndNextState(
        i1i2: (I, I2),
        stateBeforeProcessingI1I2: State
    ): ((O, O2), State) = {
      val (o1, state1AfterProcesingI1) =
        self.presentAndNextState(i1i2._1, stateBeforeProcessingI1I2._1)
      val (o2, state2AfterProcesingI2) =
        scan2.presentAndNextState(i1i2._2, stateBeforeProcessingI1I2._2)
      ((o1, o2), (state1AfterProcesingI1, state2AfterProcesingI2))

    }
  }

  def join[I2 <: I, O2](scan2: Scan[I2, O2]): Scan[I2, (O, O2)] = new Scan[I2, (O, O2)] {
    override type State = (self.State, scan2.State)

    override def initialState: State = (self.initialState, scan2.initialState)

    override def presentAndNextState(i: I2, stateBeforeProcessingI: State): ((O, O2), State) = {
      val (o1, state1AfterProcesingI1) = self.presentAndNextState(i, stateBeforeProcessingI._1)
      val (o2, state2AfterProcesingI2) = scan2.presentAndNextState(i, stateBeforeProcessingI._2)
      ((o1, o2), (state1AfterProcesingI1, state2AfterProcesingI2))
    }
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

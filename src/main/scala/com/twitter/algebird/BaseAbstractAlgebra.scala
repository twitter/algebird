/*
Copyright 2012 Twitter, Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
package com.twitter.algebird

import scala.annotation.tailrec

import java.lang.{Integer => JInt, Short => JShort, Long => JLong, Float => JFloat, Double => JDouble, Boolean => JBool}
import java.util.{List => JList, Map => JMap}

/**
 * Semigroup:
 *   This is a class with a plus method that is associative: a+(b+c) = (a+b)+c
 *
 * Monoid (take a deep breath, and relax about the weird name):
 *   This is a semigroup that has an additive identity (called zero), such that a+0=a, 0+a=a, for every a
 *
 * Group: this is a monoid that also has subtraction (and negation):
 *   So, you can do (a-b), or -a (which is equal to 0 - a).
 *
 * Ring: Group + multiplication (see: http://en.wikipedia.org/wiki/Ring_%28mathematics%29)
 *  and the three elements it defines:
 *  - additive identity aka zero
 *  - addition
 *  - multiplication
 *
 * The ring is to be passed as an argument to Matrix innerproduct and
 * provides the definitions for addition and multiplication
 *
 * Field: Ring + division. It is a generalization of Ring and adds support for inversion and
 *   multiplicative identity.
 */

trait Semigroup[@specialized(Int,Long,Float,Double) T] extends java.io.Serializable {
  def plus(l : T, r : T) : T
}

trait Monoid[@specialized(Int,Long,Float,Double) T] extends Semigroup[T] {
  def zero : T //additive identity
  def assertNotZero(v : T) {
    if(!isNonZero(v)) {
      throw new java.lang.IllegalArgumentException("argument should not be zero")
    }
  }

  def isNonZero(v : T) = (v != zero)

  def nonZeroOption(v : T): Option[T] = {
    if (isNonZero(v)) {
      Some(v)
    }
    else {
      None
    }
  }

  // Left sum: (((a + b) + c) + d)
  def sum(iter : Traversable[T]) : T = {
    iter.foldLeft(zero) { (old, current) => plus(old, current) }
  }
}

trait Group[@specialized(Int,Long,Float,Double) T] extends Monoid[T] {
  // must override negate or minus (or both)
  def negate(v : T) : T = minus(zero, v)
  def minus(l : T, r : T) : T = plus(l, negate(r))
}

trait Ring[@specialized(Int,Long,Float,Double) T] extends Group[T] {
  def one : T // Multiplicative identity
  def times(l : T, r : T) : T
  // Left product: (((a * b) * c) * d)
  def product(iter : Traversable[T]) : T = {
    iter.foldLeft(one) { (old, current) => times(old, current) }
  }
}

trait Field[@specialized(Int,Long,Float,Double) T] extends Ring[T] {
  // default implementation uses div YOU MUST OVERRIDE ONE OF THESE
  def inverse(v : T) : T = {
    assertNotZero(v)
    div(one, v)
  }
  // default implementation uses inverse:
  def div(l : T, r : T) : T = {
    assertNotZero(r)
    times(l, inverse(r))
  }
}

// a + b == max(a, b)
class MaxSemigroup[T](implicit ord : Ordering[T]) extends Semigroup[T] {
  def plus(l : T, r : T) = ord.max(l, r)
}

// a + b == min(a, b)
class MinSemigroup[T](implicit ord : Ordering[T]) extends Semigroup[T] {
  def plus(l : T, r : T) = ord.min(l, r)
}

/**
 * Some(5) + Some(3) == Some(8)
 * Some(5) + None == Some(5)
 */
class OptionMonoid[T](implicit semi : Semigroup[T]) extends Monoid[Option[T]] {
  def zero = None
  def plus(left : Option[T], right : Option[T]) : Option[T] = {
    if(left.isEmpty) {
      right
    }
    else if(right.isEmpty) {
      left
    }
    else {
      Some(semi.plus(left.get, right.get))
    }
  }
}

/** Either semigroup is useful for error handling.
 * if everything is correct, use Right (it's right, get it?), if something goes
 * wrong, use Left.  plus does the normal thing for plus(Right,Right), or plus(Left,Left),
 * but if exactly one is Left, we return that value (to keep the error condition).
 * Typically, the left value will be a string representing the errors.
 */
class EitherSemigroup[L,R](implicit semigroupl : Semigroup[L], semigroupr : Semigroup[R])
  extends Semigroup[Either[L,R]] {
  override def plus(l : Either[L,R], r : Either[L,R]) = {
    if(l.isLeft) {
      // l is Left, r may or may not be:
      if(r.isRight) {
        //Avoid the allocation:
        l
      }
      else {
        //combine the lefts:
        Left(semigroupl.plus(l.left.get, r.left.get))
      }
    }
    else if(r.isLeft) {
      //l is not a Left value, so just return right:
      r
    }
    else {
      //both l and r are Right values:
      Right(semigroupr.plus(l.right.get, r.right.get))
    }
  }
}

/**
 * If I add implicit before semigroupl, scala complains:
 * BaseAbstractAlgebra.scala:166: `implicit' modifier cannot be used for top-level object
 * which doesn't seem to make sense
 */
class EitherMonoid[L,R](semigroupl : Semigroup[L], monoidr : Monoid[R])
  extends EitherSemigroup()(semigroupl, monoidr) with Monoid[Either[L,R]] {
  override lazy val zero = Right(monoidr.zero)
}

object StringMonoid extends Monoid[String] {
  override val zero = ""
  override def plus(left : String, right : String) = left + right
}

/** List concatenation monoid.
 * plus means concatenation, zero is empty list
 */
class ListMonoid[T] extends Monoid[List[T]] {
  override def zero = List[T]()
  override def plus(left : List[T], right : List[T]) = left ++ right
}

/** A sorted-take List monoid (not the default, you can set:
 * implicit val sortmon = new SortedTakeListMonoid[T](10)
 * to use this instead of the standard list
 * This returns the k least values:
 * equivalent to: (left ++ right).sorted.take(k)
 * but doesn't do a total sort
 */
class SortedTakeListMonoid[T](k : Int)(implicit ord : Ordering[T]) extends Monoid[List[T]] {
  override def zero = List[T]()
  override def plus(left : List[T], right : List[T]) : List[T] = {
    //This is the internal loop that does one comparison:
    @tailrec
    def mergeSortR(acc : List[T], list1 : List[T], list2 : List[T], k : Int) : List[T] = {
      (list1, list2, k) match {
        case (_,_,0) => acc
        case (x1 :: t1, x2 :: t2, _) => {
          if( ord.lt(x1,x2) ) {
            mergeSortR(x1 :: acc, t1, list2, k-1)
          }
          else {
            mergeSortR(x2 :: acc, list1, t2, k-1)
          }
        }
        case (x1 :: t1, Nil, _) => mergeSortR(x1 :: acc, t1, Nil, k-1)
        case (Nil, x2 :: t2, _) => mergeSortR(x2 :: acc, Nil, t2, k-1)
        case (Nil, Nil, _) => acc
      }
    }
    mergeSortR(Nil, left, right, k).reverse
  }
}

/** Set union monoid.
 * plus means union, zero is empty set
 */
class SetMonoid[T] extends Monoid[Set[T]] {
  override def zero = Set[T]()
  override def plus(left : Set[T], right : Set[T]) = left ++ right
}

/** Function1 monoid.
 * plus means function composition, zero is the identity function
 */
class Function1Monoid[T] extends Monoid[Function1[T,T]] {
  override def zero = {
    (t : T) => t
  }

  // (f1 + f2)(x) = f2(f1(x)) so that:
  // listOfFn.foldLeft(x) { (v, fn) => fn(v) } = (Monoid.sum(listOfFn))(x)
  override def plus(f1 : Function1[T,T], f2 : Function1[T,T]) = {
    (t : T) => f2(f1(t))
  }
}

/** You can think of this as a Sparse vector monoid
 */
class MapMonoid[K,V](implicit monoid : Monoid[V]) extends Monoid[Map[K,V]] {
  override lazy val zero = Map[K,V]()
  override def isNonZero(x : Map[K,V]) = !x.isEmpty && x.valuesIterator.exists { v =>
    monoid.isNonZero(v)
  }
  override def plus(x : Map[K,V], y : Map[K,V]) = {
    // Scala maps can reuse internal structure, so don't copy just add into the bigger one:
    // This really saves computation when adding lots of small maps into big ones (common)
    val (big, small, bigOnLeft) = if(x.size > y.size) { (x,y,true) } else { (y,x,false) }
    small.foldLeft(big) { (oldMap, kv) =>
      val newV = big
        .get(kv._1)
        .map { bigV => if(bigOnLeft) monoid.plus(bigV, kv._2) else monoid.plus(kv._2, bigV) }
        .getOrElse(kv._2)
      if (monoid.isNonZero(newV)) {
        oldMap + (kv._1 -> newV)
      }
      else {
        oldMap - kv._1
      }
    }
  }
  // This is not part of the typeclass, but it is useful:
  def sumValues(elem : Map[K,V]) : V = elem.values.reduceLeft { monoid.plus(_,_) }
}

/** You can think of this as a Sparse vector group
 */
class MapGroup[K,V](implicit vgrp : Group[V]) extends MapMonoid[K,V]()(vgrp) with Group[Map[K,V]] {
  override def negate(kv : Map[K,V]) = kv.mapValues { v => vgrp.negate(v) }
}

/** You can think of this as a Sparse vector ring
 */
class MapRing[K,V](implicit ring : Ring[V]) extends MapGroup[K,V]()(ring) with Ring[Map[K,V]] {
  // It is possible to implement this, but we need a special "identity map" which we
  // deal with as if it were map with all possible keys (.get(x) == ring.one for all x).
  // Then we have to manage the delta from this map as we add elements.  That said, it
  // is not actually needed in matrix multiplication, so we are punting on it for now.
  override def one = sys.error("multiplicative identity for Map unimplemented")
  override def times(x : Map[K,V], y : Map[K,V]) : Map[K,V] = {
    val (big, small, bigOnLeft) = if(x.size > y.size) { (x,y,true) } else { (y,x,false) }
    small.foldLeft(zero) { (oldMap, kv) =>
      val bigV = big.getOrElse(kv._1, ring.zero)
      val newV = if(bigOnLeft) ring.times(bigV, kv._2) else ring.times(kv._2, bigV)
      if (ring.isNonZero(newV)) {
        oldMap + (kv._1 -> newV)
      }
      else {
        oldMap - kv._1
      }
    }
  }
  // This is not part of the typeclass, but it is useful:
  def productValues(elem : Map[K,V]) : V = elem.values.reduceLeft { ring.times(_,_) }
  def dot(left : Map[K,V], right : Map[K,V]) : V = sumValues(times(left, right))
}

object IntRing extends Ring[Int] {
  override def zero = 0
  override def one = 1
  override def negate(v : Int) = -v
  override def plus(l : Int, r : Int) = l + r
  override def minus(l : Int, r : Int) = l - r
  override def times(l : Int, r : Int) = l * r
}

object ShortRing extends Ring[Short] {
  override def zero = 0.toShort
  override def one = 1.toShort
  override def negate(v : Short) = (-v).toShort
  override def plus(l : Short, r : Short) = (l + r).toShort
  override def minus(l : Short, r : Short) = (l - r).toShort
  override def times(l : Short, r : Short) = (l * r).toShort
}

object LongRing extends Ring[Long] {
  override def zero = 0L
  override def one = 1L
  override def negate(v : Long) = -v
  override def plus(l : Long, r : Long) = l + r
  override def minus(l : Long, r : Long) = l - r
  override def times(l : Long, r : Long) = l * r
}

object FloatField extends Field[Float] {
  override def one = 1.0f
  override def zero = 0.0f
  override def negate(v : Float) = -v
  override def plus(l : Float, r : Float) = l + r
  override def minus(l : Float, r : Float) = l - r
  override def times(l : Float, r : Float) = l * r
  override def div(l : Float, r : Float) = {
    assertNotZero(r)
    l / r
  }
}

object DoubleField extends Field[Double] {
  override def one = 1.0
  override def zero = 0.0
  override def negate(v : Double) = -v
  override def plus(l : Double, r : Double) = l + r
  override def minus(l : Double, r : Double) = l - r
  override def times(l : Double, r : Double) = l * r
  override def div(l : Double, r : Double) = {
    assertNotZero(r)
    l / r
  }
}

object BooleanField extends Field[Boolean] {
  override def one = true
  override def zero = false
  override def negate(v : Boolean) = v
  override def plus(l : Boolean, r : Boolean) = l ^ r
  override def minus(l : Boolean, r : Boolean) = l ^ r
  override def times(l : Boolean, r : Boolean) = l && r
  override def inverse(l : Boolean) = {
    assertNotZero(l)
    true
  }
  override def div(l : Boolean, r : Boolean) = {
    assertNotZero(r)
    l
  }
}

// Trivial group, but possibly useful to make a group of (Unit, T) for some T.
object UnitGroup extends Group[Unit] {
  override def zero = ()
  override def negate(u : Unit) = ()
  override def plus(l : Unit, r : Unit) = ()
}

// similar to the above:
object NullGroup extends Group[Null] {
  override def zero = null
  override def negate(u : Null) = null
  override def plus(l : Null, r : Null) = null
}

object Semigroup extends GeneratedSemigroupImplicits {
  // This pattern is really useful for typeclasses
  def plus[T](l : T, r : T)(implicit semi : Semigroup[T]) = semi.plus(l,r)

  implicit val nullSemigroup : Semigroup[Null] = NullGroup
  implicit val unitSemigroup : Semigroup[Unit] = UnitGroup
  implicit val boolSemigroup : Semigroup[Boolean] = BooleanField
  implicit val jboolSemigroup : Semigroup[JBool] = JBoolField
  implicit val intSemigroup : Semigroup[Int] = IntRing
  implicit val jintSemigroup : Semigroup[JInt] = JIntRing
  implicit val shortSemigroup : Semigroup[Short] = ShortRing
  implicit val jshortSemigroup : Semigroup[JShort] = JShortRing
  implicit val longSemigroup : Semigroup[Long] = LongRing
  implicit val jlongSemigroup : Semigroup[JLong] = JLongRing
  implicit val floatSemigroup : Semigroup[Float] = FloatField
  implicit val jfloatSemigroup : Semigroup[JFloat] = JFloatField
  implicit val doubleSemigroup : Semigroup[Double] = DoubleField
  implicit val jdoubleSemigroup : Semigroup[JDouble] = JDoubleField
  implicit val stringSemigroup : Semigroup[String] = StringMonoid
  implicit def optionSemigroup[T : Semigroup] : Semigroup[Option[T]] = new OptionMonoid[T]
  implicit def listSemigroup[T] : Semigroup[List[T]] = new ListMonoid[T]
  implicit def indexedSeqSemigroup[T : Monoid]: Semigroup[IndexedSeq[T]] = new IndexedSeqMonoid[T]
  implicit def jlistSemigroup[T] : Semigroup[JList[T]] = new JListMonoid[T]
  implicit def setSemigroup[T] : Semigroup[Set[T]] = new SetMonoid[T]
  implicit def mapSemigroup[K,V](implicit mon : Monoid[V]) : Semigroup[Map[K, V]] = new MapMonoid[K,V]
  implicit def jmapSemigroup[K,V : Monoid] : Semigroup[JMap[K, V]] = new JMapMonoid[K,V]
  implicit def eitherSemigroup[L : Semigroup, R : Semigroup] = new EitherSemigroup[L,R]
  implicit def function1Semigroup[T] : Semigroup[Function1[T,T]] = new Function1Monoid[T]
}

object Monoid extends GeneratedMonoidImplicits {
  // This pattern is really useful for typeclasses
  def zero[T](implicit mon : Monoid[T]) = mon.zero
  def assertNotZero[T](v: T)(implicit monoid: Monoid[T]) = monoid.assertNotZero(v)
  def isNonZero[T](v: T)(implicit monoid: Monoid[T]) = monoid.isNonZero(v)
  def nonZeroOption[T](v: T)(implicit monoid: Monoid[T]) = monoid.nonZeroOption(v)
  def sum[T](iter: Traversable[T])(implicit monoid: Monoid[T]) = monoid.sum(iter)

  implicit val nullMonoid : Monoid[Null] = NullGroup
  implicit val unitMonoid : Monoid[Unit] = UnitGroup
  implicit val boolMonoid : Monoid[Boolean] = BooleanField
  implicit val jboolMonoid : Monoid[JBool] = JBoolField
  implicit val intMonoid : Monoid[Int] = IntRing
  implicit val jintMonoid : Monoid[JInt] = JIntRing
  implicit val shortMonoid : Monoid[Short] = ShortRing
  implicit val jshortMonoid : Monoid[JShort] = JShortRing
  implicit val longMonoid : Monoid[Long] = LongRing
  implicit val jlongMonoid : Monoid[JLong] = JLongRing
  implicit val floatMonoid : Monoid[Float] = FloatField
  implicit val jfloatMonoid : Monoid[JFloat] = JFloatField
  implicit val doubleMonoid : Monoid[Double] = DoubleField
  implicit val jdoubleMonoid : Monoid[JDouble] = JDoubleField
  implicit val stringMonoid : Monoid[String] = StringMonoid
  implicit def optionMonoid[T : Monoid] = new OptionMonoid[T]
  implicit def listMonoid[T] : Monoid[List[T]] = new ListMonoid[T]
  implicit def indexedSeqMonoid[T:Monoid]: Monoid[IndexedSeq[T]] = new IndexedSeqMonoid[T]
  implicit def jlistMonoid[T] : Monoid[JList[T]] = new JListMonoid[T]
  implicit def setMonoid[T] : Monoid[Set[T]] = new SetMonoid[T]
  implicit def mapMonoid[K,V](implicit mon : Monoid[V]) = new MapMonoid[K,V]
  implicit def jmapMonoid[K,V : Monoid] = new JMapMonoid[K,V]
  implicit def eitherMonoid[L, R](implicit l : Semigroup[L], r : Monoid[R]) : Monoid[Either[L, R]] = new EitherMonoid[L,R](l, r)
  implicit def function1Monoid[T] = new Function1Monoid[T]
}

object Group extends GeneratedGroupImplicits {
  // This pattern is really useful for typeclasses
  def minus[T](l : T, r : T)(implicit grp : Group[T]) = grp.minus(l,r)

  implicit val nullGroup : Group[Null] = NullGroup
  implicit val unitGroup : Group[Unit] = UnitGroup
  implicit val boolGroup : Group[Boolean] = BooleanField
  implicit val jboolGroup : Group[JBool] = JBoolField
  implicit val intGroup : Group[Int] = IntRing
  implicit val jintGroup : Group[JInt] = JIntRing
  implicit val shortGroup : Group[Short] = ShortRing
  implicit val jshortGroup : Group[JShort] = JShortRing
  implicit val longGroup : Group[Long] = LongRing
  implicit val jlongGroup : Group[JLong] = JLongRing
  implicit val floatGroup : Group[Float] = FloatField
  implicit val jfloatGroup : Group[JFloat] = JFloatField
  implicit val doubleGroup : Group[Double] = DoubleField
  implicit val jdoubleGroup : Group[JDouble] = JDoubleField
  implicit def indexedSeqGroup[T:Group]: Group[IndexedSeq[T]] = new IndexedSeqGroup[T]
  implicit def mapGroup[K,V](implicit group : Group[V]) = new MapGroup[K,V]()(group)
}

object Ring extends GeneratedRingImplicits {
  // This pattern is really useful for typeclasses
  def one[T](implicit rng : Ring[T]) = rng.one
  def times[T](l : T, r : T)(implicit rng : Ring[T]) = rng.times(l,r)
  def product[T](it : Traversable[T])(implicit rng : Ring[T]) = rng.product(it)

  implicit val boolRing : Ring[Boolean] = BooleanField
  implicit val jboolRing : Ring[JBool] = JBoolField
  implicit val intRing : Ring[Int] = IntRing
  implicit val jintRing : Ring[JInt] = JIntRing
  implicit val shortRing : Ring[Short] = ShortRing
  implicit val jshortRing : Ring[JShort] = JShortRing
  implicit val longRing : Ring[Long] = LongRing
  implicit val jlongRing : Ring[JLong] = JLongRing
  implicit val floatRing : Ring[Float] = FloatField
  implicit val jfloatRing : Ring[JFloat] = JFloatField
  implicit val doubleRing : Ring[Double] = DoubleField
  implicit val jdoubleRing : Ring[JDouble] = JDoubleField
  implicit def indexedSeqRing[T:Ring]: Ring[IndexedSeq[T]] = new IndexedSeqRing[T]
  implicit def mapRing[K,V](implicit ring : Ring[V]) = new MapRing[K,V]()(ring)
}

object Field {
  // This pattern is really useful for typeclasses
  def div[T](l : T, r : T)(implicit fld : Field[T]) = fld.div(l,r)

  implicit val boolField : Field[Boolean] = BooleanField
  implicit val jboolField : Field[JBool] = JBoolField
  implicit val floatField : Field[Float] = FloatField
  implicit val jfloatField : Field[JFloat] = JFloatField
  implicit val doubleField : Field[Double] = DoubleField
  implicit val jdoubleField : Field[JDouble] = JDoubleField
}

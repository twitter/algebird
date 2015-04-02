package com.twitter.algebird

import org.scalacheck.Prop._
import org.scalacheck.{ Arbitrary, Gen }

import scala.annotation.tailrec

class RightFolded2Test extends CheckProperties {
  import com.twitter.algebird.BaseProperties._

  def monFold(i: Int, l: Long) = l + i.toLong
  def mapFn(l: Long) = l / 2

  implicit val rightFoldedHasAdditionOperatorAndZero = RightFolded2.monoid[Int, Long, Long](mapFn)(monFold)

  def rightFolded2Value[In, Out, Acc](implicit arbout: Arbitrary[Out], mon: RightFolded2HasAdditionOperatorAndZero[In, Out, Acc]): Gen[RightFoldedValue2[In, Out, Acc]] =
    for (v <- arbout.arbitrary) yield mon.init(v)

  def rightFolded2ToFold[In, Out, Acc](implicit arbin: Arbitrary[In], mon: RightFolded2HasAdditionOperatorAndZero[In, Out, Acc]): Gen[RightFoldedToFold2[In]] =
    for (v <- arbin.arbitrary) yield mon.toFold(v)

  implicit def rightFolded2[In, Out, Acc](implicit arbin: Arbitrary[In], arbout: Arbitrary[Out], mon: RightFolded2HasAdditionOperatorAndZero[In, Out, Acc]): Arbitrary[RightFolded2[In, Out, Acc]] =
    Arbitrary { Gen.oneOf(rightFolded2Value[In, Out, Acc], rightFolded2ToFold[In, Out, Acc]) }

  property("RightFolded2 is a monoid") {
    monoidLaws[RightFolded2[Int, Long, Long]]
  }

  // Make a list of lists such that the all but the last element satisfies the predicate
  // and joining the lists returns the original list
  @tailrec
  private[this] def chunk[T](items: List[T], acc: List[List[T]] = Nil)(pred: T => Boolean): List[List[T]] = {
    val (headL, tailL) = items.span(pred)
    if (tailL.isEmpty) {
      if (!headL.isEmpty) (headL :: acc).reverse else acc.reverse
    } else {
      val newAcc = (headL :+ (tailL.head)) :: acc
      chunk(tailL.tail, newAcc)(pred)
    }
  }
  // The last element in this list must be a rightFoldedValue
  def fold[In, Out, Acc](l: List[RightFolded2[In, Out, Acc]])(foldfn: (In, Out) => Out): Option[Out] = {
    l.last match {
      case RightFoldedValue2(v, _, _) => {
        Some(l.dropRight(1)
          .flatMap { _.asInstanceOf[RightFoldedToFold2[In]].in }
          .foldRight(v)(foldfn))
      }
      case _ => None
    }
  }

  def sum[In, Out, Acc: Group](l: List[RightFolded2[In, Out, Acc]])(foldfn: (In, Out) => Out)(mapfn: (Out) => Acc): Acc = {
    def notIsVal(rf: RightFolded2[In, Out, Acc]) = rf match {
      case RightFoldedValue2(_, _, _) => false
      case _ => true
    }
    val chunks = chunk(l)(notIsVal)

    val grp = implicitly[Group[Acc]]
    val vals = chunks.map { fold(_)(foldfn).map(mapfn).getOrElse(grp.zero) }
    grp.sum(vals)
  }

  def accOf[In, Out, Acc](rfv: RightFolded2[In, Out, Acc]): Option[Acc] = {
    rfv match {
      case RightFoldedValue2(_, acc, _) => Some(acc)
      case _ => None
    }
  }

  property("RightFolded2 sum works as expected") {
    forAll { (ls: List[RightFolded2[Int, Long, Long]]) =>
      val accSum = accOf(rightFoldedHasAdditionOperatorAndZero.sum(ls)).getOrElse(0L)
      sum(ls)(monFold)(mapFn) == accSum
    }
  }
}
